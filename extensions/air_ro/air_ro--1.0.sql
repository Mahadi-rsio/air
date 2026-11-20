/* air_ro is loaded via shared_preload_libraries = 'air_ro' */

CREATE FUNCTION air_ro_status(
    OUT enabled bool,
    OUT queries_allowed int8,
    OUT writes_blocked int8,
    OUT allow_temp_tables bool
)
RETURNS record
AS 'MODULE_PATHNAME', 'air_ro_status'
LANGUAGE C STRICT VOLATILE;
