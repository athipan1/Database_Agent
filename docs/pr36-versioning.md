# Observation Versioning

Each successful non-replay observation advances the promotion version exactly
once. Identical retries return the stored result snapshot. Conflicting requests
using an old expected version fail as stale and cannot overwrite the winner.
