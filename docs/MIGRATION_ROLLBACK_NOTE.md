# Rollback note

Rollback should restore an application release that expects the currently applied schema or apply an explicit down migration where one exists. Runtime startup must never be used as a rollback mechanism.
