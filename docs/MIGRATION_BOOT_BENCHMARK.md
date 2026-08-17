# Boot benchmark target

The previous Supabase probe spent roughly 44 seconds in startup schema setup. After migrations-first startup, schema verification should be a single read-only identity lookup. CI enforces a conservative post-migration API boot target below 15 seconds; a real Supabase probe should record the final production-equivalent number before merge.
