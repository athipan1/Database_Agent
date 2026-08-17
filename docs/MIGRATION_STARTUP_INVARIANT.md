# Startup invariant

Normal production startup is SELECT-only with respect to schema state. No `CREATE`, `ALTER`, ensure-column helper, table setup routine, or partition creation job belongs in the application lifespan.
