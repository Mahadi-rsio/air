package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"air-stage2/pkg/cell"
	"air-stage2/pkg/crypto"
	"air-stage2/pkg/density"
	"air-stage2/pkg/failover"
	"air-stage2/pkg/observability"
)

type Stage2Report struct {
	TimestampUTC    string                               `json:"timestamp_utc"`
	ExpC2           *density.ExpC2Result                 `json:"exp_c2_multi_tenant_density_and_bombs"`
	ExpC4           *density.ExpC4Result                 `json:"exp_c4_shared_buffers_maintenance_and_finops"`
	ExpC5D2D3D6     *failover.ExpC5D2D3D6Result          `json:"exp_c5_d2_d3_d6_ha_failover_and_split_brain"`
	ObservabilityDR *observability.ObservabilityDRResult `json:"observability_and_dr_suite"`
	AllPassed       bool                                 `json:"all_passed"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	ctx := context.Background()

	log.Println("=== STARTING STAGE 2: MULTI-TENANT DENSITY, PRO SYNC HA FAILOVER & OBSERVABILITY ===")

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		log.Fatalf("NewEnvelopeKMS failed: %v", err)
	}

	agent, err := cell.NewAgent("/vol", 3, kms)
	if err != nil {
		log.Fatalf("cell.NewAgent failed: %v", err)
	}
	defer agent.Close()

	s3c, bucket, err := cell.NewR2ClientFromEnv(ctx)
	if err != nil {
		log.Printf("[stage2] warning: R2 client init: %v", err)
	}

	log.Println("--- [1/4] Running EXP-C2: Multi-Tenant Density, OOM/Fork Bomb Containment & Auto-Scaler ---")
	c2Res, err := density.RunExpC2(ctx, agent)
	if err != nil {
		log.Fatalf("EXP-C2 failed: %v", err)
	}
	log.Printf("[EXP-C2] pass=%v active=%d avg_rss=%.2fMB oom_contained=%v (%s) fork_53300=%d tenant_b_ok=%d/fail=%d",
		c2Res.Pass, c2Res.ActiveClustersStarted, c2Res.AvgActiveClusterRSSMB,
		c2Res.OOMBombContained, c2Res.OOMBombSQLState, c2Res.ForkStormRejected53300,
		c2Res.TenantBQueriesSucceeded, c2Res.TenantBQueriesFailed)

	log.Println("--- [2/4] Running EXP-C4: shared_buffers Curve, Anti-Wraparound maintenance_lock & 4-Way FinOps Separation ---")
	c4Res, err := density.RunExpC4(ctx, agent)
	if err != nil {
		log.Fatalf("EXP-C4 failed: %v", err)
	}
	log.Printf("[EXP-C4] pass=%v maint_lock_veto=%v post_freeze_xid_age=%d ru_variance=%.2f%% r2_cost_reduction=%.1f%%",
		c4Res.Pass, c4Res.MaintenanceLockVetoedSleep, c4Res.PostFreezeRealXIDAge,
		c4Res.CustomerRUVariancePct, c4Res.R2ClassACostReductionPct)

	log.Println("--- [3/4] Running EXP-C5, EXP-D2, EXP-D3 & EXP-D6: Crash Recovery, Pro Sync HA, R2 Spool & Asymmetric Split-Brain Fencing ---")
	failoverRes, err := failover.RunExpC5D2D3D6(ctx, agent, kms, s3c, bucket)
	if err != nil {
		log.Fatalf("EXP-C5/D2/D3/D6 failed: %v", err)
	}
	log.Printf("[EXP-C5/D2/D3/D6] pass=%v c5_rows=%d d2_stalled=%v d3_drained=%d d6_kill_ms=%.1f < promote_ms=%.1f (divergent=%d)",
		failoverRes.Pass, failoverRes.C5RecoveredRowsAfterKill, failoverRes.D2CommitStalledOnPartition,
		failoverRes.D3DrainedWALSegmentsToR2, failoverRes.D6AsyncOldPrimaryKillMs,
		failoverRes.D6AsyncNewPrimaryPromoteMs, failoverRes.D6AsyncDivergentCommitsPost)

	log.Println("--- [4/4] Running Observability (22 Signals, 15 Alerts, 50k Cardinality) & 12 DR Runbook Drills ---")
	obsRes, err := observability.RunObservabilityAndDRSuite(ctx, kms, "/etc/air/alertmanager-rules.yaml", "/etc/air/DR-01-to-12.md")
	if err != nil {
		log.Fatalf("Observability & DR suite failed: %v", err)
	}
	log.Printf("[OBS-DR] pass=%v metrics=%d/22 alerts=%d/15 dr_runbooks=%d/12 kek_rotated=%d cardinality_reduction=%.1f%%",
		obsRes.Pass, obsRes.ExposedAuditMetricsCount, obsRes.AlertmanagerRulesLoaded,
		obsRes.DRRunbooksVerifiedCount, obsRes.DR11KEKRotatedTenants,
		obsRes.UQR11CardinalityAt50kTenants.CardinalityReductionPct)

	report := Stage2Report{
		TimestampUTC:    time.Now().UTC().Format(time.RFC3339),
		ExpC2:           c2Res,
		ExpC4:           c4Res,
		ExpC5D2D3D6:     failoverRes,
		ObservabilityDR: obsRes,
		AllPassed:       c2Res.Pass && c4Res.Pass && failoverRes.Pass && obsRes.Pass,
	}

	outBytes, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatalf("json.MarshalIndent failed: %v", err)
	}
	fmt.Println("=== STAGE 2 JSON REPORT BEGIN ===")
	fmt.Println(string(outBytes))
	fmt.Println("=== STAGE 2 JSON REPORT END ===")

	if !report.AllPassed {
		os.Exit(1)
	}
}
