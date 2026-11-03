---- MODULE KeptPublication ----
EXTENDS Naturals, Sequences, FiniteSets
VARIABLES scenario, phase, durable, executing, record, observed, deletedNewer
vars == <<scenario,phase,durable,executing,record,observed,deletedNewer>>
Init == /\ scenario \in {"launch","listing"} /\ phase = "Preparing" /\ durable = FALSE /\ executing = FALSE
        /\ record = 1 /\ observed = 0 /\ deletedNewer = FALSE
Commit == /\ scenario = "launch" /\ phase = "Preparing" /\ durable' = TRUE /\ phase' = "Recorded"
          /\ UNCHANGED <<scenario,executing,record,observed,deletedNewer>>
Execute == /\ scenario = "launch" /\ phase = "Recorded" /\ executing' = TRUE /\ phase' = "Running"
           /\ UNCHANGED <<scenario,durable,record,observed,deletedNewer>>
InspectStale == /\ scenario = "listing" /\ observed = 0 /\ record = 1 /\ observed' = 1
  /\ UNCHANGED <<scenario,phase,durable,executing,record,deletedNewer>>
ReplaceRecord == /\ scenario = "listing" /\ observed = 1 /\ record = 1 /\ record' = 2
  /\ UNCHANGED <<scenario,phase,durable,executing,observed,deletedNewer>>
Prune == /\ scenario = "listing" /\ observed # 0 /\ record = observed
         /\ deletedNewer' = (record # observed) /\ record' = 0
         /\ UNCHANGED <<scenario,phase,durable,executing,observed>>
Next == Commit \/ Execute \/ InspectStale \/ ReplaceRecord \/ Prune
RecordedBeforeExec == executing => durable
PreserveNewerRecord == ~deletedNewer
====
