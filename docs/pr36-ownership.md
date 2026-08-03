# Lifecycle Ownership

Database_Agent owns promotion state, version, transitions, observation ledger,
expiration, revocation, and exact evidence lookup.

Manager_Agent owns scheduled paper observation, correlation propagation, broker
and Database reconciliation, and orchestration of Risk_Agent and Execution_Agent.

Risk_Agent owns execution approval and emergency halt policy.

Execution_Agent is the only component permitted to call the broker.
