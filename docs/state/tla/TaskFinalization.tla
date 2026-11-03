---- MODULE TaskFinalization ----
EXTENDS Naturals, Sequences, FiniteSets
VARIABLES phase, cause, cleanup, published
vars == <<phase, cause, cleanup, published>>
Init == /\ phase = "Starting" /\ cause = "none" /\ cleanup = "pending" /\ published = FALSE
Attach == /\ phase = "Starting" /\ phase' = "Running" /\ UNCHANGED <<cause,cleanup,published>>
Stop(c) == /\ phase \in {"Starting","Running"} /\ cause' = c /\ phase' = "Cleaning"
           /\ UNCHANGED <<cleanup,published>>
Settle(outcome) == /\ phase = "Cleaning" /\ cleanup' = outcome /\ phase' = "Collecting"
                  /\ UNCHANGED <<cause,published>>
Publish == /\ phase = "Collecting" /\ published' = TRUE /\ phase' = "Finished"
           /\ UNCHANGED <<cause,cleanup>>
Next == Attach \/ Publish \/ (\E c \in {"exit","cancel","timeout","failure"}: Stop(c))
        \/ (\E o \in {"verified","unverified","error"}: Settle(o))
PublishAfterCleanup == published => cleanup # "pending"
CauseBeforeFinished == published => cause # "none"
====
