//! Read-only diagnostic snapshots for a failed handle-growth assertion.
use ntapi::ntpsapi::{
    NtQueryInformationProcess, ProcessHandleInformation, PROCESS_HANDLE_SNAPSHOT_INFORMATION,
    PROCESS_HANDLE_TABLE_ENTRY_INFO,
};
use std::{collections::BTreeMap, mem::size_of};

pub fn describe(label: &str) {
    // usize storage gives the native table proper alignment. Bound diagnostics to 1 MiB.
    let mut table = vec![0usize; 1024 * 1024 / size_of::<usize>()];
    let mut required = 0;
    let status = unsafe {
        NtQueryInformationProcess(
            windows_sys::Win32::System::Threading::GetCurrentProcess().cast(),
            ProcessHandleInformation,
            table.as_mut_ptr().cast(),
            (table.len() * size_of::<usize>()) as u32,
            &mut required,
        )
    };
    if status < 0 {
        eprintln!("handle types {label}: unavailable status={status:x}");
        return;
    }
    let snapshot = unsafe { &*table.as_ptr().cast::<PROCESS_HANDLE_SNAPSHOT_INFORMATION>() };
    let count = snapshot.NumberOfHandles;
    let header = 2 * size_of::<usize>();
    if count
        > (table.len() * size_of::<usize>() - header) / size_of::<PROCESS_HANDLE_TABLE_ENTRY_INFO>()
    {
        return;
    }
    let entries = unsafe {
        std::slice::from_raw_parts(
            table
                .as_ptr()
                .cast::<u8>()
                .add(header)
                .cast::<PROCESS_HANDLE_TABLE_ENTRY_INFO>(),
            count,
        )
    };
    let mut types = BTreeMap::<String, usize>::new();
    for entry in entries {
        let mut info = vec![0usize; 512];
        let result = unsafe {
            ntapi::ntobapi::NtQueryObject(
                entry.HandleValue,
                ntapi::ntobapi::ObjectTypeInformation,
                info.as_mut_ptr().cast(),
                (info.len() * size_of::<usize>()) as u32,
                &mut required,
            )
        };
        let mut name = format!("type#{}", entry.ObjectTypeIndex);
        if result >= 0 {
            let data = unsafe {
                &*info
                    .as_ptr()
                    .cast::<ntapi::ntobapi::OBJECT_TYPE_INFORMATION>()
            };
            let address = data.TypeName.Buffer as usize;
            let length = data.TypeName.Length as usize;
            let start = info.as_ptr() as usize;
            if length > 0
                && address >= start
                && address.saturating_add(length) <= start + info.len() * size_of::<usize>()
            {
                name = String::from_utf16_lossy(unsafe {
                    std::slice::from_raw_parts(data.TypeName.Buffer, length / 2)
                });
            }
        }
        *types.entry(name).or_default() += 1;
    }
    eprintln!("handle types {label}: {types:?}");
}
