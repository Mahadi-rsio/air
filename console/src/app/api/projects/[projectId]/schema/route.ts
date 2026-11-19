import { NextRequest, NextResponse } from "next/server";
import {
  authenticateRequest,
  handleApiError,
  requireProjectAccess,
} from "@/lib/auth/server";
import { AirBackendUnavailableError } from "@/lib/air-client";
import { withProjectPgClient } from "@/lib/pg-client";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "databases:query");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    const selectedSchema = req.nextUrl.searchParams.get("schema");
    const selectedTable = req.nextUrl.searchParams.get("table");

    const data = await withProjectPgClient(project, async (client) => {
      const schemasRes = await client.query(`
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_toast', 'pg_catalog', 'information_schema')
        ORDER BY schema_name;
      `);

      const tablesRes = await client.query(`
        SELECT
          n.nspname AS schema_name,
          c.relname AS table_name,
          CASE c.relkind
            WHEN 'r' THEN 'table'
            WHEN 'p' THEN 'partitioned_table'
            WHEN 'v' THEN 'view'
            WHEN 'm' THEN 'materialized_view'
            WHEN 'S' THEN 'sequence'
          END AS object_type,
          COALESCE(s.n_live_tup, 0) AS row_estimate,
          pg_total_relation_size(c.oid) AS total_bytes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema')
          AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
        ORDER BY n.nspname, c.relname;
      `);

      const functionsRes = await client.query(`
        SELECT
          n.nspname AS schema_name,
          p.proname AS function_name,
          pg_get_function_result(p.oid) AS return_type
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema')
        ORDER BY n.nspname, p.proname
        LIMIT 200;
      `);

      const enumsRes = await client.query(`
        SELECT
          n.nspname AS schema_name,
          t.typname AS enum_name,
          array_agg(e.enumlabel ORDER BY e.enumsortorder) AS labels
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        GROUP BY n.nspname, t.typname
        ORDER BY n.nspname, t.typname;
      `);

      let tableDetails = null;
      if (selectedSchema && selectedTable) {
        const colsRes = await client.query(
          `
          SELECT
            column_name,
            data_type,
            udt_name,
            is_nullable,
            column_default,
            character_maximum_length
          FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = $2
          ORDER BY ordinal_position;
          `,
          [selectedSchema, selectedTable]
        );

        const constraintsRes = await client.query(
          `
          SELECT
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
          FROM information_schema.table_constraints tc
          LEFT JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
          LEFT JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
          WHERE tc.table_schema = $1 AND tc.table_name = $2
          ORDER BY tc.constraint_type, tc.constraint_name;
          `,
          [selectedSchema, selectedTable]
        );

        const indexesRes = await client.query(
          `
          SELECT indexname, indexdef
          FROM pg_indexes
          WHERE schemaname = $1 AND tablename = $2
          ORDER BY indexname;
          `,
          [selectedSchema, selectedTable]
        );

        tableDetails = {
          schema: selectedSchema,
          table: selectedTable,
          columns: colsRes.rows,
          constraints: constraintsRes.rows,
          indexes: indexesRes.rows,
        };
      }

      return {
        schemas: schemasRes.rows.map((r) => r.schema_name as string),
        objects: tablesRes.rows,
        functions: functionsRes.rows,
        enums: enumsRes.rows,
        tableDetails,
      };
    });

    return NextResponse.json(data);
  } catch (err) {
    if (err instanceof AirBackendUnavailableError) {
      return NextResponse.json(
        { error: err.message, code: "AIR_BACKEND_UNAVAILABLE" },
        { status: err.statusCode }
      );
    }
    return handleApiError(err);
  }
}
