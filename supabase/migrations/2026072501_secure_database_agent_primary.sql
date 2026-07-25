-- Run after Database_Agent's idempotent setup_runtime_tables bootstrap.
-- Database_Agent connects directly with the database owner through PostgreSQL.
-- The Supabase Data API is deliberately denied access to trading tables.

begin;

alter default privileges for role postgres in schema public
    revoke select, insert, update, delete, truncate, references, trigger
    on tables from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema public
    revoke usage, select, update on sequences
    from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema public
    revoke execute on functions
    from public, anon, authenticated, service_role;

revoke all privileges on all tables in schema public
    from public, anon, authenticated, service_role;
revoke all privileges on all sequences in schema public
    from public, anon, authenticated, service_role;
revoke all privileges on all functions in schema public
    from public, anon, authenticated, service_role;

alter function public.apply_order_strategy_bucket_assignment()
    set search_path = public, pg_temp;
alter function public.apply_position_strategy_bucket_assignment()
    set search_path = public, pg_temp;
alter function public.maintain_position_highest_price_since_entry()
    set search_path = public, pg_temp;

DO $$
DECLARE
    item record;
    policy_name text;
BEGIN
    FOR item IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format(
            'ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY',
            item.schemaname,
            item.tablename
        );
        policy_name := 'deny_data_api_' || substr(md5(item.tablename), 1, 12);
        IF NOT EXISTS (
            SELECT 1
            FROM pg_policies
            WHERE schemaname = item.schemaname
              AND tablename = item.tablename
              AND policyname = policy_name
        ) THEN
            EXECUTE format(
                'CREATE POLICY %I ON %I.%I FOR ALL TO anon, authenticated, service_role USING (false) WITH CHECK (false)',
                policy_name,
                item.schemaname,
                item.tablename
            );
        END IF;
    END LOOP;
END
$$;

commit;
