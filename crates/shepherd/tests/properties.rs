//! Generated state-machine traces over the pure domain and the contract adapter.
use proptest::prelude::*;
use shepherd::{NullBackend, SupervisorBuilder, TerminateOptions};
use shepherd_domain::*;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(shepherd_test_support::TestEnvironment::from_env().property_cases()))]
    #[test]
    fn domain_invariants_one_through_nine(trace in prop::collection::vec((any::<bool>(), 0u8..5), 0..100)) {
        let mut scopes = [ProcessScope::new(ProcessScopeId::new(1)), ProcessScope::new(ProcessScopeId::new(2))];
        let mut owner = BTreeMap::new(); let mut next = 0u64;
        let mut reaps = BTreeMap::<ProcessId,usize>::new();
        for (second,action) in trace {
            let i = usize::from(second); let other = format!("{:?}", scopes[1-i]);
            let scope = &mut scopes[i];
            match action {
                0 => {
                    next += 1; let pid = ProcessId::new(next);
                    let attached = scope.attach_spawned(pid, OsIdentity::new(next as u32, ReuseToken::StartTime(next)), ProcessSpec::new("model"));
                    if attached.is_ok() { prop_assert!(owner.insert(pid,i).is_none()); } else { prop_assert!(!scope.is_open()); }
                }
                1 => { scope.begin_scope_termination(); }
                2 => { for pid in scope.process_ids() { scope.request_termination(pid).unwrap(); scope.request_termination(pid).unwrap(); } }
                3 => { for pid in scope.process_ids() {
                    scope.record_exit(pid).unwrap();
                    let exit = ProcessExit { pid, code: Some(0), signal: None, outcome: TerminationOutcome::ExitedNaturally, forced: false };
                    for _ in 0..2 { for event in scope.record_reaped(pid,exit).unwrap() { if matches!(event,DomainEvent::ProcessReaped{..}) { *reaps.entry(pid).or_default() += 1; } } }
                } }
                _ => { for pid in scope.process_ids() { scope.prune(pid); } }
            }
            prop_assert_eq!(format!("{:?}", scopes[1-i]), other, "cross-scope mutation");
            for (j,s) in scopes.iter().enumerate() { for pid in s.process_ids() { prop_assert_eq!(owner[&pid],j); prop_assert!(!scopes[1-j].contains(pid)); } }
            prop_assert!(reaps.values().all(|n| *n == 1));
        }
        // Complete every remaining owned child, then verify no terminal bookkeeping remains.
        for scope in &mut scopes { scope.begin_scope_termination(); for pid in scope.process_ids() {
            scope.record_reaped(pid, ProcessExit { pid, code: Some(0), signal: None, outcome: TerminationOutcome::ForcedRequired, forced: true }).unwrap(); scope.prune(pid);
        } prop_assert!(scope.process_ids().is_empty()); prop_assert!(scope.live_process_ids().is_empty()); }
    }

    #[test]
    fn contract_invariants_one_through_nine(programs in prop::collection::vec((any::<bool>(), any::<bool>()), 0..24)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let backend = Arc::new(NullBackend::new());
            let sup = SupervisorBuilder::new().backend(backend).build();
            let scopes = [sup.create_scope(),sup.create_scope()]; let mut pids = [Vec::new(),Vec::new()];
            for (second, stubborn) in programs {
                let i=usize::from(second);
                let pid=sup.spawn(scopes[i],ProcessSpec::new(if stubborn {"ignore-graceful"} else {"respect-graceful"})).await.unwrap();
                assert!(!pids[1-i].contains(&pid)); pids[i].push(pid);
            }
            let options=TerminateOptions { grace: GracePeriod::new(Duration::ZERO),force_timeout:Some(Duration::from_secs(1)) };
            for i in 0..2 {
                let report=sup.terminate_scope(scopes[i],options).await.unwrap(); assert!(report.all_verified());
                assert_eq!(sup.terminate_scope(scopes[i],options).await.unwrap().outcomes,report.outcomes);
                assert!(sup.spawn(scopes[i],ProcessSpec::new("rejected")).await.is_err());
                assert!(sup.processes(scopes[i]).is_none());
                for pid in &pids[i] { assert!(sup.wait(*pid).await.unwrap().outcome.is_verified()); }
                if i==0 { assert_eq!(sup.processes(scopes[1]).unwrap(),pids[1]); }
            }
            sup.shutdown().await.unwrap();
        });
    }
}
