# Release migration gate

A Database_Agent release is deployable only when its migration command succeeds and the resulting schema identity matches the release constants. API readiness is a second, read-only verification of that state.
