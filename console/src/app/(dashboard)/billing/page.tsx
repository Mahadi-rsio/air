"use client";

import * as React from "react";
import { CreditCard, AlertTriangle } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function BillingPage() {
  return (
    <div className="space-y-5 max-w-5xl mx-auto">
      <div>
        <h1 className="text-lg font-semibold tracking-tight">Usage & Billing</h1>
        <p className="text-xs text-[var(--text-secondary)]">
          Compute, storage, data transfer metering, and invoice history.
        </p>
      </div>

      <div className="flex items-start gap-2.5 rounded-lg border border-[var(--warning-border)] bg-[var(--warning-soft)] p-4 text-xs text-[var(--warning)]">
        <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
        <div>
          <p className="font-medium">
            Usage & Billing Metering API Pending Backend Integration
          </p>
          <p className="mt-1 text-[var(--text-secondary)]">
            <code>air-server</code> v2.0 does not yet expose compute-unit metering, R2 storage billing, or invoice endpoints. Per AIR Console policy, no synthetic pricing or usage numbers are fabricated.
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle>Compute Usage</CardTitle>
          </CardHeader>
          <CardContent className="text-xs text-[var(--text-secondary)]">
            Awaiting <code>GET /v1/billing/usage</code>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Storage & R2 Backups</CardTitle>
          </CardHeader>
          <CardContent className="text-xs text-[var(--text-secondary)]">
            Awaiting <code>GET /v1/billing/usage</code>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-1.5">
              <CreditCard className="h-3.5 w-3.5" /> Invoices
            </CardTitle>
          </CardHeader>
          <CardContent className="text-xs text-[var(--text-secondary)]">
            Awaiting <code>GET /v1/billing/invoices</code>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
