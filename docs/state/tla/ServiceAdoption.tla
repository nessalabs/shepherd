---- MODULE ServiceAdoption ----
EXTENDS Naturals, Sequences, FiniteSets
VARIABLES phase, matches, mayAdopt, priorCleanup, adopted, replaced
vars == <<phase,matches,mayAdopt,priorCleanup,adopted,replaced>>
Init == /\ phase = "Inspecting" /\ matches \in BOOLEAN /\ mayAdopt \in BOOLEAN
        /\ priorCleanup = "pending" /\ adopted = FALSE /\ replaced = FALSE
Adopt == /\ phase = "Inspecting" /\ matches /\ mayAdopt
         /\ adopted' = TRUE /\ phase' = "Running"
         /\ UNCHANGED <<matches,mayAdopt,priorCleanup,replaced>>
Terminate(outcome) == /\ phase = "Inspecting" /\ ~(matches /\ mayAdopt)
  /\ priorCleanup' = outcome /\ phase' = "Terminated"
  /\ UNCHANGED <<matches,mayAdopt,adopted,replaced>>
Replace == /\ phase = "Terminated" /\ priorCleanup = "verified"
           /\ phase' = "Running" /\ replaced' = TRUE
           /\ UNCHANGED <<matches,mayAdopt,priorCleanup,adopted>>
Next == Adopt \/ Replace \/ (\E o \in {"verified","unverified"}: Terminate(o))
AdoptionMatches == adopted => matches
OnlyFirstAdoption == adopted => mayAdopt
ReplacementVerified == replaced => priorCleanup = "verified"
====
