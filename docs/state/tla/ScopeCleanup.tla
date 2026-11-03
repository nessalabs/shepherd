---- MODULE ScopeCleanup ----
EXTENDS Naturals, Sequences, FiniteSets
VARIABLES phase, evidence, tracked, verifiedEver
vars == <<phase, evidence, tracked, verifiedEver>>
P == {1, 2}
Init == /\ phase = "Open" /\ evidence = [p \in P |-> "none"]
        /\ tracked = P /\ verifiedEver = {}
Drain == /\ phase = "Open" /\ phase' = "Draining"
         /\ UNCHANGED <<evidence, tracked, verifiedEver>>
Observe(p, outcome) == /\ p \in tracked /\ evidence[p] # "verified"
  /\ evidence' = [evidence EXCEPT ![p] = outcome]
  /\ verifiedEver' = (IF outcome = "verified" THEN verifiedEver \cup {p} ELSE verifiedEver)
  /\ UNCHANGED <<phase, tracked>>
Close == /\ phase = "Draining"
         /\ \A p \in tracked : evidence[p] = "verified"
         /\ phase' = "Closed" /\ UNCHANGED <<evidence, tracked, verifiedEver>>
Prune(p) == /\ p \in tracked /\ evidence[p] = "verified"
            /\ tracked' = tracked \ {p} /\ UNCHANGED <<phase, evidence, verifiedEver>>
Next == Drain \/ Close \/ (\E p \in P : Prune(p))
        \/ (\E p \in P, o \in {"unverified", "verified"} : Observe(p,o))
ClosedVerified == phase = "Closed" => \A p \in P : evidence[p] = "verified"
PrunedVerified == \A p \in P \ tracked : evidence[p] = "verified"
VerifiedNeverLost == \A p \in verifiedEver : evidence[p] = "verified"
====
