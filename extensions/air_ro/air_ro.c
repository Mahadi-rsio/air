/*-------------------------------------------------------------------------
 *
 * air_ro.c
 *	  Native PostgreSQL 17 read-only execution enforcement extension for AIR.
 *
 * Enforces strict read-only execution at the planner/executor, utility, and
 * catalog object-access layers when air_ro.enabled = on. All persistent
 * mutations (DML, DDL, modifying CTEs, row locks, COPY FROM, DO/CALL, etc.)
 * are rejected with ERRCODE_READ_ONLY_SQL_TRANSACTION before touching storage.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

typedef struct AirRoShmemState
{
	pg_atomic_uint64 queries_allowed;
	pg_atomic_uint64 writes_blocked;
} AirRoShmemState;

static bool air_ro_enabled = false;
static bool air_ro_allow_temp_tables = false;

static AirRoShmemState *air_ro_shmem = NULL;
static uint64 local_queries_allowed = 0;
static uint64 local_writes_blocked = 0;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static object_access_hook_type prev_object_access_hook = NULL;

PG_FUNCTION_INFO_V1(air_ro_status);

static inline void
air_ro_inc_allowed(void)
{
	if (air_ro_shmem != NULL)
		pg_atomic_fetch_add_u64(&air_ro_shmem->queries_allowed, 1);
	else
		local_queries_allowed++;
}

static inline void
air_ro_inc_blocked(void)
{
	if (air_ro_shmem != NULL)
		pg_atomic_fetch_add_u64(&air_ro_shmem->writes_blocked, 1);
	else
		local_writes_blocked++;
}

static void
air_ro_reject_write(const char *reason)
{
	air_ro_inc_blocked();
	ereport(ERROR,
			(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
			 errmsg("AIR read-only engine rejected write operation: %s", reason)));
}

static bool
air_ro_rowmarks_require_lock(List *rowMarks)
{
	ListCell   *lc;

	foreach(lc, rowMarks)
	{
		PlanRowMark *rc = lfirst_node(PlanRowMark, lc);

		if (RowMarkRequiresRowShareLock(rc->markType))
			return true;
	}
	return false;
}

static void
air_ro_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (air_ro_enabled && queryDesc != NULL && queryDesc->plannedstmt != NULL)
	{
		PlannedStmt *ps = queryDesc->plannedstmt;

		if (ps->commandType != CMD_SELECT)
			air_ro_reject_write("DML statements (INSERT/UPDATE/DELETE/MERGE) are not permitted");

		if (ps->hasModifyingCTE)
			air_ro_reject_write("data-modifying CTEs are not permitted");

		if (air_ro_rowmarks_require_lock(ps->rowMarks))
			air_ro_reject_write("SELECT ... FOR UPDATE/SHARE row locking is not permitted");

		/*
		 * Defense-in-depth: ensure the active transaction is marked read-only
		 * during execution even if a GUC or subtransaction tried to alter it.
		 */
		if (!XactReadOnly)
			XactReadOnly = true;

		air_ro_inc_allowed();
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
air_ro_check_transaction_stmt(TransactionStmt *stmt)
{
	ListCell   *lc;

	if (stmt->kind != TRANS_STMT_BEGIN && stmt->kind != TRANS_STMT_START)
		return;

	foreach(lc, stmt->options)
	{
		DefElem    *opt = lfirst_node(DefElem, lc);

		if (strcmp(opt->defname, "transaction_read_only") == 0)
		{
			bool		ro = true;

			if (opt->arg != NULL && IsA(opt->arg, A_Const))
			{
				A_Const    *val = castNode(A_Const, opt->arg);

				if (IsA(&val->val, Boolean))
					ro = boolVal(&val->val);
				else if (IsA(&val->val, Integer))
					ro = (intVal(&val->val) != 0);
			}
			else
				ro = defGetBoolean(opt);

			if (!ro)
				air_ro_reject_write("READ WRITE transactions are not permitted");
		}
	}
}

static void
air_ro_check_variable_set_stmt(VariableSetStmt *stmt)
{
	if (stmt->name == NULL)
		return;

	if (pg_strcasecmp(stmt->name, "transaction_read_only") == 0 ||
		pg_strcasecmp(stmt->name, "default_transaction_read_only") == 0)
	{
		if (stmt->kind == VAR_RESET || stmt->kind == VAR_RESET_ALL)
			air_ro_reject_write("cannot reset read-only transaction mode");

		if (stmt->kind == VAR_SET_VALUE && list_length(stmt->args) == 1)
		{
			A_Const    *val = linitial_node(A_Const, stmt->args);

			if (IsA(&val->val, Boolean) && !boolVal(&val->val))
				air_ro_reject_write("cannot disable transaction_read_only");
			if (IsA(&val->val, String))
			{
				const char *s = strVal(&val->val);

				if (pg_strcasecmp(s, "off") == 0 ||
					pg_strcasecmp(s, "false") == 0 ||
					pg_strcasecmp(s, "0") == 0)
					air_ro_reject_write("cannot disable transaction_read_only");
			}
		}
	}
}

static void
air_ro_check_utility(Node *parsetree)
{
	switch (nodeTag(parsetree))
	{
		case T_ExplainStmt:
		case T_PrepareStmt:
		case T_ExecuteStmt:
		case T_DeallocateStmt:
		case T_VariableShowStmt:
		case T_DiscardStmt:
		case T_DeclareCursorStmt:
		case T_FetchStmt:
		case T_ClosePortalStmt:
		case T_ListenStmt:
		case T_UnlistenStmt:
			/* Allowed; underlying queries still pass through ExecutorStart_hook. */
			return;

		case T_TransactionStmt:
			air_ro_check_transaction_stmt(castNode(TransactionStmt, parsetree));
			return;

		case T_VariableSetStmt:
			air_ro_check_variable_set_stmt(castNode(VariableSetStmt, parsetree));
			return;

		case T_CopyStmt:
			{
				CopyStmt   *stmt = castNode(CopyStmt, parsetree);

				if (stmt->is_from)
					air_ro_reject_write("COPY FROM is not permitted");
				return;
			}

		case T_CreateStmt:
			{
				CreateStmt *stmt = castNode(CreateStmt, parsetree);

				if (air_ro_allow_temp_tables &&
					stmt->relation != NULL &&
					stmt->relation->relpersistence == RELPERSISTENCE_TEMP)
					return;
				air_ro_reject_write("CREATE TABLE is not permitted");
			}
			break;

		case T_CreateTableAsStmt:
			{
				CreateTableAsStmt *stmt = castNode(CreateTableAsStmt, parsetree);

				if (air_ro_allow_temp_tables &&
					stmt->into != NULL &&
					stmt->into->rel != NULL &&
					stmt->into->rel->relpersistence == RELPERSISTENCE_TEMP)
					return;
				air_ro_reject_write("CREATE TABLE AS / SELECT INTO is not permitted");
			}
			break;

		default:
			air_ro_reject_write("DDL and mutating utility commands are not permitted");
			break;
	}
}

static void
air_ro_ProcessUtility(PlannedStmt *pstmt,
					  const char *queryString,
					  bool readOnlyTree,
					  ProcessUtilityContext context,
					  ParamListInfo params,
					  QueryEnvironment *queryEnv,
					  DestReceiver *dest,
					  QueryCompletion *qc)
{
	if (air_ro_enabled && pstmt != NULL && pstmt->utilityStmt != NULL)
		air_ro_check_utility(pstmt->utilityStmt);

	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString, readOnlyTree,
							context, params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv, dest, qc);
}

static void
air_ro_object_access(ObjectAccessType access,
					 Oid classId,
					 Oid objectId,
					 int subId,
					 void *arg)
{
	if (prev_object_access_hook)
		prev_object_access_hook(access, classId, objectId, subId, arg);

	if (!air_ro_enabled)
		return;

	if (access == OAT_POST_CREATE ||
		access == OAT_DROP ||
		access == OAT_POST_ALTER)
	{
		if (air_ro_allow_temp_tables && classId == RelationRelationId)
		{
			/* Allow temp table catalog entries when explicitly enabled. */
			return;
		}
		air_ro_reject_write("persistent catalog mutation is not permitted");
	}
}

static void
air_ro_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(MAXALIGN(sizeof(AirRoShmemState)));
}

static void
air_ro_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	air_ro_shmem = ShmemInitStruct("air_ro_shmem",
								   sizeof(AirRoShmemState),
								   &found);
	if (!found)
	{
		pg_atomic_init_u64(&air_ro_shmem->queries_allowed, 0);
		pg_atomic_init_u64(&air_ro_shmem->writes_blocked, 0);
	}
}

Datum
air_ro_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};
	uint64		allowed;
	uint64		blocked;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "air_ro_status: return type must be a row type");

	if (air_ro_shmem != NULL)
	{
		allowed = pg_atomic_read_u64(&air_ro_shmem->queries_allowed);
		blocked = pg_atomic_read_u64(&air_ro_shmem->writes_blocked);
	}
	else
	{
		allowed = local_queries_allowed;
		blocked = local_writes_blocked;
	}

	values[0] = BoolGetDatum(air_ro_enabled);
	values[1] = Int64GetDatum((int64) allowed);
	values[2] = Int64GetDatum((int64) blocked);
	values[3] = BoolGetDatum(air_ro_allow_temp_tables);

	return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls));
}

void
_PG_init(void)
{
	DefineCustomBoolVariable("air_ro.enabled",
							 "Enable AIR native read-only execution enforcement.",
							 NULL,
							 &air_ro_enabled,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("air_ro.allow_temp_tables",
							 "Allow ephemeral TEMP tables on read-only engine.",
							 NULL,
							 &air_ro_allow_temp_tables,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	if (process_shared_preload_libraries_in_progress)
	{
		prev_shmem_request_hook = shmem_request_hook;
		shmem_request_hook = air_ro_shmem_request;

		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = air_ro_shmem_startup;
	}

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = air_ro_ExecutorStart;

	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = air_ro_ProcessUtility;

	prev_object_access_hook = object_access_hook;
	object_access_hook = air_ro_object_access;
}
