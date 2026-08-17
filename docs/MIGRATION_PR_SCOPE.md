# PR scope

This PR changes deployment/startup schema ownership only. It does not change trading API contracts or trading policy. The measurable target is to remove schema mutation from API boot and reduce production-equivalent startup latency from the previous DDL-heavy baseline.
