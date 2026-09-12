// Read-only accounting experiment; creates and reaps only its own child.
// macOS: clang -Wall -Wextra -O2 macos-rusage-probe.c -o /tmp/rusage-probe
#include <assert.h>
#include <errno.h>
#include <libproc.h>
#include <mach/mach_time.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

static struct rusage_info_v4 sample(pid_t pid) {
    struct rusage_info_v4 r = {0};
    assert(proc_pid_rusage(pid, RUSAGE_INFO_V4, (rusage_info_t *)&r) == 0);
    return r;
}
static double seconds(struct rusage *r) {
    return r->ru_utime.tv_sec + r->ru_stime.tv_sec +
        (r->ru_utime.tv_usec + r->ru_stime.tv_usec) / 1e6;
}
int main(void) {
    mach_timebase_info_data_t tb;
    assert(mach_timebase_info(&tb) == 0);
    printf("Mach timebase: %u/%u\n", tb.numer, tb.denom);
    for (int flavor = 0; flavor <= 4; ++flavor) {
        struct rusage_info_v4 r = {0};
        int rc = proc_pid_rusage(getpid(), flavor, (rusage_info_t *)&r);
        printf("flavor %d: rc=%d\n", flavor, rc);
        assert(rc == 0);
    }
    struct rusage_info_v4 before = sample(getpid());
    struct rusage a, b;
    assert(getrusage(RUSAGE_SELF, &a) == 0);
    volatile unsigned long work = 1;
    uint64_t start = mach_absolute_time();
    while ((mach_absolute_time() - start) * (double)tb.numer / tb.denom < 400000000.0)
        work = work * 1664525 + 1013904223;
    assert(getrusage(RUSAGE_SELF, &b) == 0);
    struct rusage_info_v4 after = sample(getpid());
    double raw = (double)(after.ri_user_time - before.ri_user_time) +
        (double)(after.ri_system_time - before.ri_system_time);
    printf("CPU: getrusage=%.6fs raw/1e9=%.6fs Mach-converted=%.6fs\n",
        seconds(&b)-seconds(&a), raw/1e9, raw*tb.numer/tb.denom/1e9);
    volatile char *memory = calloc(1, 64*1024*1024);
    assert(memory);
    for (size_t i=0; i<64*1024*1024; i+=4096) memory[i]=1;
    struct rusage_info_v4 grown = sample(getpid());
    printf("64MiB touched: RSS delta=%lld footprint delta=%lld peak footprint=%llu\n",
        (long long)grown.ri_resident_size-(long long)after.ri_resident_size,
        (long long)grown.ri_phys_footprint-(long long)after.ri_phys_footprint,
        (unsigned long long)grown.ri_lifetime_max_phys_footprint);
    FILE *file=tmpfile(); assert(file);
    struct rusage_info_v4 io_before=sample(getpid());
    assert(fwrite((const void *)memory, 1, 8*1024*1024, file)==8*1024*1024);
    assert(fflush(file)==0); assert(fsync(fileno(file))==0);
    struct rusage_info_v4 io_after=sample(getpid());
    printf("8MiB file write: disk-write delta=%llu logical-write delta=%llu\n",
        (unsigned long long)(io_after.ri_diskio_byteswritten-io_before.ri_diskio_byteswritten),
        (unsigned long long)(io_after.ri_logical_writes-io_before.ri_logical_writes));
    fclose(file);
    free((void *)memory);
    int ready[2], finish[2]; assert(pipe(ready)==0); assert(pipe(finish)==0);
    pid_t child=fork(); assert(child>=0);
    if (child==0) {
        close(ready[0]); close(finish[1]);
        assert(write(ready[1], "x", 1)==1);
        char c; assert(read(finish[0], &c, 1)==1);
        _exit(0);
    }
    close(ready[1]); close(finish[0]); char c;
    assert(read(ready[0], &c, 1)==1);
    sample(child); puts("external-to-Shepherd child: live query succeeded");
    assert(write(finish[1], "x", 1)==1);
    siginfo_t si={0}; assert(waitid(P_PID, child, &si, WEXITED|WNOWAIT)==0);
    struct rusage_info_v4 dead=sample(child);
    printf("zombie query succeeded; exit timestamp=%llu\n", (unsigned long long)dead.ri_proc_exit_abstime);
    assert(waitpid(child, NULL, 0)==child);
    errno=0; int rc=proc_pid_rusage(child, RUSAGE_INFO_V4, (rusage_info_t *)&dead);
    printf("after reap: rc=%d errno=%d\n", rc, errno);
    assert(rc==-1 && errno==ESRCH);
    errno=0; rc=proc_pid_rusage(1, RUSAGE_INFO_V4, (rusage_info_t *)&dead);
    printf("PID 1 access: rc=%d errno=%d\n", rc, errno);
    close(ready[0]); close(finish[1]);
    return 0;
}
