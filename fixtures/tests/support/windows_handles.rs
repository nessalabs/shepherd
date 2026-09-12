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
    // SAFETY: the initialized usize allocation is aligned for the NT record,
    // its exact byte capacity is supplied, and required is a writable length.
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
    // SAFETY: successful query filled an aligned allocation larger than the
    // fixed header; integer/raw-pointer fields accept every initialized bit pattern.
    let snapshot = unsafe { &*table.as_ptr().cast::<PROCESS_HANDLE_SNAPSHOT_INFORMATION>() };
    let count = snapshot.NumberOfHandles;
    let header = 2 * size_of::<usize>();
    if count
        > (table.len() * size_of::<usize>() - header) / size_of::<PROCESS_HANDLE_TABLE_ENTRY_INFO>()
    {
        return;
    }
    // SAFETY: count was bounded against the allocation; the header offset and
    // entry stride preserve native alignment and table stays alive for this slice.
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
        // SAFETY: query uses an initialized aligned buffer with exact capacity;
        // a stale snapshot handle can fail or report another type, but is never closed.
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
            // SAFETY: the successful query filled the fixed record in aligned,
            // initialized storage; the string pointer is validated separately below.
            let data = unsafe {
                &*info
                    .as_ptr()
                    .cast::<ntapi::ntobapi::OBJECT_TYPE_INFORMATION>()
            };
            let address = data.TypeName.Buffer as usize;
            let length = data.TypeName.Length as usize;
            let start = info.as_ptr() as usize;
            if length > 0
                && length % 2 == 0
                && address % std::mem::align_of::<u16>() == 0
                && address >= start
                && address.saturating_add(length) <= start + info.len() * size_of::<usize>()
            {
                // SAFETY: pointer alignment, even byte length, and containment in
                // the still-live info allocation have all been checked above.
                name = String::from_utf16_lossy(unsafe {
                    std::slice::from_raw_parts(data.TypeName.Buffer, length / 2)
                });
            }
        }
        *types.entry(name).or_default() += 1;
    }
    eprintln!("handle types {label}: {types:?}");
}
