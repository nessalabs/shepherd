---- MODULE OutputBudget ----
EXTENDS Naturals, Sequences, FiniteSets
VARIABLES queued, tail, produced, consumed, dropped, writes
vars == <<queued,tail,produced,consumed,dropped,writes>>
Min(a,b) == IF a < b THEN a ELSE b
Init == /\ queued = 0 /\ tail = 0 /\ produced = 0 /\ consumed = 0 /\ dropped = 0 /\ writes = 0
Write(n) == /\ writes < 3 /\ queued' = Min(2,queued+n)
  /\ tail' = Min(1,tail+n) /\ produced' = produced+n
  /\ dropped' = dropped + queued+n-Min(2,queued+n)
  /\ writes' = writes+1 /\ UNCHANGED consumed
Read == /\ queued > 0 /\ consumed' = consumed+queued /\ queued' = 0
        /\ UNCHANGED <<tail,produced,dropped,writes>>
Next == Read \/ (\E n \in {1,2}: Write(n))
QueueBound == queued <= 2
TailBound == tail <= 1
ByteAccounting == produced = consumed+queued+dropped
====
