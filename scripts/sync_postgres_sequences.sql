-- Synchronize serial sequences after restoring data into Supabase.
-- Safe for empty tables and quoted identifiers.

DO $$
DECLARE
    item record;
    maximum_value bigint;
BEGIN
    FOR item IN
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            a.attname AS column_name,
            pg_get_serial_sequence(
                format('%I.%I', n.nspname, c.relname),
                a.attname
            ) AS sequence_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND pg_get_serial_sequence(
                format('%I.%I', n.nspname, c.relname),
                a.attname
              ) IS NOT NULL
    LOOP
        EXECUTE format(
            'SELECT max(%I)::bigint FROM %I.%I',
            item.column_name,
            item.schema_name,
            item.table_name
        ) INTO maximum_value;

        IF maximum_value IS NULL THEN
            PERFORM setval(item.sequence_name::regclass, 1, false);
        ELSE
            PERFORM setval(item.sequence_name::regclass, maximum_value, true);
        END IF;
    END LOOP;
END
$$;
