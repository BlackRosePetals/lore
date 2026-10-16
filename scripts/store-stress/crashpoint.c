// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//
// An LD_PRELOAD shim that kills its process, or fails a file operation, at an exact point in the
// file operations a store makes, for the store stress suite. See README.md beside it.
//
// It counts the file operations on paths containing $STRESS_PATH, in the order the process makes
// them, and then, as the environment asks:
//
//   STRESS_CRASH_AT=N      SIGKILL the process once operation N is done
//   STRESS_CRASH_BEFORE=1  ... before doing it instead
//   STRESS_TEAR=1          ... with only part of a write N done, as a kill during a large write
//                           can leave it
//   STRESS_FAIL_AT=N       fail operation N, and the STRESS_FAIL_COUNT - 1 after it, with
//   STRESS_FAIL_ERRNO=E,.. the first of these errnos the operation can meet on its own, or EIO
//                           when none is (see `natural`). A failed write or read moves part of its
//                           bytes and returns the short count instead when STRESS_SHORT=1
//   STRESS_FAIL_PROB=P     fail each operation with probability P instead, from STRESS_SEED
//   STRESS_OPS=a,b         count only these: write, sync, rename, truncate, unlink, open, read,
//                           stat, list, mkdir (default: the five that change the disk)
//   STRESS_CLASS=a,b       count only operations on these kinds of file (see `classify`)
//   STRESS_DURING=a,b      count only operations made while one of these store operations runs,
//                           as the harness registers them; `idle` for while none does
//   STRESS_TRACE=file      append one line per counted operation to file
//   STRESS_COUNT=file      write the number of counted operations to file at exit
//   STRESS_PLACES=file     write at exit, or at the kill, how many operations of each kind, file
//                           kind and running store operations were counted, and which were failed
//   STRESS_KILLED=file     write where the kill landed to file
//
// The harness registers the store operations it runs with `crashpoint_register`, and ends the
// faults with `crashpoint_disarm` to check that the store recovers from them in the same process.
//
// The store uses the psync I/O backend by default, which makes every file operation through
// libc; the uring backend bypasses the shim for reads and writes.
//
// Build: cc -O2 -shared -fPIC -o crashpoint.so crashpoint.c -ldl

#define _GNU_SOURCE
#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#define MAX_FDS 65536
#define MAX_KINDS 32

enum op {
    OP_WRITE = 1,
    OP_SYNC = 2,
    OP_RENAME = 4,
    OP_TRUNCATE = 8,
    OP_UNLINK = 16,
    OP_OPEN = 32,
    OP_READ = 64,
    OP_STAT = 128,
    OP_LIST = 256,
    OP_MKDIR = 512,
};
static const char *const op_names[] = {"write", "sync", "rename", "truncate", "unlink",
                                       "open", "read", "stat", "list", "mkdir"};
#define OP_COUNT 10

// The kinds of file a store keeps, by name. A rename takes the kind of the file it renames, so
// that installing a bucket written to a temporary file and installing a `.new` file of a fan-out
// commit are told apart.
enum class_id {
    C_PACK,
    C_BUCKET,
    C_BUCKET_TMP,
    C_NEW,
    C_NEW_TMP,
    C_LEVEL,
    C_LEVEL_TMP,
    C_PENDING,
    C_PENDING_TMP,
    C_COMPACT,
    C_COMPACT_TMP,
    C_ROOT_FILE,
    C_DIR,
    C_OTHER,
    C_COUNT
};
static const char *const class_names[] = {"pack",    "bucket",      "bucket.tmp",  "new",
                                          "new.tmp", "level",       "level.tmp",   "pending",
                                          "pending.tmp", "compact", "compact.tmp", "root-file",
                                          "dir",     "other"};

static const char *filter;
static int ops_mask = OP_WRITE | OP_SYNC | OP_RENAME | OP_TRUNCATE | OP_UNLINK;
static unsigned class_mask = ~0u;
static long crash_at = -1, fail_at = -1, fail_count = 1;
static int crash_before, tear, short_io;
static int fail_errnos[8], fail_errno_count;
static double fail_prob;
static uint64_t rng_state = 0x9E3779B97F4A7C15ull;
static int trace_fd = -1;
static const char *count_path, *places_path, *killed_path;
static long counter, faults;
static unsigned char watched[MAX_FDS];
static int initialized, disarmed;

// The store operations the harness runs: a count per kind, which it updates, and their names.
static const volatile size_t *running;
static int kind_count;
static char kind_names[MAX_KINDS][24];
static const char *during_spec;
static uint64_t during_mask;
static int during_idle, during_set;

static ssize_t (*real_write)(int, const void *, size_t);
static ssize_t (*real_pwrite)(int, const void *, size_t, off_t);
static ssize_t (*real_pwrite64)(int, const void *, size_t, off_t);
static ssize_t (*real_writev)(int, const struct iovec *, int);
static ssize_t (*real_pwritev)(int, const struct iovec *, int, off_t);
static ssize_t (*real_pwritev64)(int, const struct iovec *, int, off_t);
static ssize_t (*real_pwritev2)(int, const struct iovec *, int, off_t, int);
static ssize_t (*real_read)(int, void *, size_t);
static ssize_t (*real_pread)(int, void *, size_t, off_t);
static ssize_t (*real_pread64)(int, void *, size_t, off_t);
static ssize_t (*real_readv)(int, const struct iovec *, int);
static ssize_t (*real_preadv)(int, const struct iovec *, int, off_t);
static ssize_t (*real_preadv64)(int, const struct iovec *, int, off_t);
static ssize_t (*real_preadv2)(int, const struct iovec *, int, off_t, int);
static int (*real_fsync)(int);
static int (*real_fdatasync)(int);
static int (*real_ftruncate)(int, off_t);
static int (*real_ftruncate64)(int, off_t);
static int (*real_rename)(const char *, const char *);
static int (*real_renameat)(int, const char *, int, const char *);
static int (*real_renameat2)(int, const char *, int, const char *, unsigned int);
static int (*real_unlink)(const char *);
static int (*real_unlinkat)(int, const char *, int);
static int (*real_open)(const char *, int, ...);
static int (*real_open64)(const char *, int, ...);
static int (*real_openat)(int, const char *, int, ...);
static int (*real_openat64)(int, const char *, int, ...);
static int (*real_close)(int);
static int (*real_statx)(int, const char *, int, unsigned int, struct statx *);
static int (*real_stat)(const char *, struct stat *);
static int (*real_stat64)(const char *, struct stat64 *);
static int (*real_lstat)(const char *, struct stat *);
static int (*real_lstat64)(const char *, struct stat64 *);
static int (*real_fstat)(int, struct stat *);
static int (*real_fstat64)(int, struct stat64 *);
static int (*real_fstatat)(int, const char *, struct stat *, int);
static int (*real_fstatat64)(int, const char *, struct stat64 *, int);
static DIR *(*real_opendir)(const char *);
static struct dirent *(*real_readdir)(DIR *);
static struct dirent64 *(*real_readdir64)(DIR *);
static int (*real_mkdir)(const char *, mode_t);
static int (*real_mkdirat)(int, const char *, mode_t);

// How many operations of each kind, on each kind of file, with each set of store operations
// running, were counted (`fault` 0) and failed (`fault` 1).
#define PLACES 8192
struct place {
    uint64_t key;
    long count;
};
static struct place places[PLACES];
static int places_lock;

static uint64_t running_mask(void) {
    uint64_t mask = 0;
    if (!running)
        return 0;
    for (int index = 0; index < kind_count; index++)
        if (__atomic_load_n(&running[index], __ATOMIC_RELAXED) > 0)
            mask |= 1ull << index;
    return mask;
}

static void note_place(int op_index, int class, uint64_t mask, int fault) {
    if (!places_path)
        return;
    uint64_t key = (mask << 16) | ((uint64_t)op_index << 9) | ((uint64_t)class << 1) | (uint64_t)fault | (1ull << 63);
    uint64_t slot = (key * 0x9E3779B97F4A7C15ull) >> 51;
    while (__atomic_exchange_n(&places_lock, 1, __ATOMIC_ACQUIRE))
        ;
    for (int probe = 0; probe < PLACES; probe++) {
        struct place *place = &places[(slot + (uint64_t)probe) % PLACES];
        if (place->key == key || place->key == 0) {
            place->key = key;
            place->count++;
            break;
        }
    }
    __atomic_store_n(&places_lock, 0, __ATOMIC_RELEASE);
}

static int format_mask(uint64_t mask, char *buffer, size_t size) {
    if (!mask)
        return snprintf(buffer, size, "idle");
    size_t used = 0;
    buffer[0] = 0;
    for (int index = 0; index < kind_count && used + 1 < size; index++) {
        if (!(mask & (1ull << index)))
            continue;
        int written = snprintf(buffer + used, size - used, "%s%s", used ? "+" : "", kind_names[index]);
        if (written < 0)
            break;
        used += (size_t)written;
    }
    return (int)used;
}

static void write_places(void) {
    if (!places_path)
        return;
    int fd = real_open(places_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
        return;
    for (int index = 0; index < PLACES; index++) {
        uint64_t key = places[index].key;
        if (!key)
            continue;
        char during[512], line[700];
        format_mask((key & ~(1ull << 63)) >> 16, during, sizeof during);
        int length = snprintf(line, sizeof line, "%s %s %s %s %ld\n", (key & 1) ? "fault" : "op",
                              op_names[(key >> 9) & 127], class_names[(key >> 1) & 255], during,
                              places[index].count);
        if (length > 0)
            real_write(fd, line, (size_t)length);
    }
    real_close(fd);
}

static void write_count(void) {
    if (!count_path)
        return;
    char buffer[32];
    int length = snprintf(buffer, sizeof buffer, "%ld\n", __atomic_load_n(&counter, __ATOMIC_SEQ_CST));
    int fd = real_open(count_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
        real_write(fd, buffer, (size_t)length);
        real_close(fd);
    }
}

static void at_exit(void) {
    write_count();
    write_places();
}

static long env_long(const char *name, long fallback) {
    const char *value = getenv(name);
    return value && *value ? strtol(value, NULL, 10) : fallback;
}

static int errno_named(const char *name, size_t length) {
    static const struct {
        const char *name;
        int value;
    } known[] = {{"EIO", EIO},       {"ENOSPC", ENOSPC}, {"EDQUOT", EDQUOT}, {"EMFILE", EMFILE},
                 {"ENFILE", ENFILE}, {"EROFS", EROFS},   {"EINTR", EINTR},   {"ENOMEM", ENOMEM},
                 {"EMLINK", EMLINK}};
    for (size_t index = 0; index < sizeof known / sizeof known[0]; index++)
        if (strlen(known[index].name) == length && !strncmp(known[index].name, name, length))
            return known[index].value;
    return 0;
}

static void *real(const char *name) { return dlsym(RTLD_NEXT, name); }

__attribute__((constructor)) static void init(void) {
    real_write = real("write");
    real_pwrite = real("pwrite");
    real_pwrite64 = real("pwrite64");
    real_writev = real("writev");
    real_pwritev = real("pwritev");
    real_pwritev64 = real("pwritev64");
    real_pwritev2 = real("pwritev2");
    real_read = real("read");
    real_pread = real("pread");
    real_pread64 = real("pread64");
    real_readv = real("readv");
    real_preadv = real("preadv");
    real_preadv64 = real("preadv64");
    real_preadv2 = real("preadv2");
    real_fsync = real("fsync");
    real_fdatasync = real("fdatasync");
    real_ftruncate = real("ftruncate");
    real_ftruncate64 = real("ftruncate64");
    real_rename = real("rename");
    real_renameat = real("renameat");
    real_renameat2 = real("renameat2");
    real_unlink = real("unlink");
    real_unlinkat = real("unlinkat");
    real_open = real("open");
    real_open64 = real("open64");
    real_openat = real("openat");
    real_openat64 = real("openat64");
    real_close = real("close");
    real_statx = real("statx");
    real_stat = real("stat");
    real_stat64 = real("stat64");
    real_lstat = real("lstat");
    real_lstat64 = real("lstat64");
    real_fstat = real("fstat");
    real_fstat64 = real("fstat64");
    real_fstatat = real("fstatat");
    real_fstatat64 = real("fstatat64");
    real_opendir = real("opendir");
    real_readdir = real("readdir");
    real_readdir64 = real("readdir64");
    real_mkdir = real("mkdir");
    real_mkdirat = real("mkdirat");

    filter = getenv("STRESS_PATH");
    if (filter && !*filter)
        filter = NULL;
    const char *ops = getenv("STRESS_OPS");
    if (ops && *ops) {
        ops_mask = 0;
        for (int index = 0; index < OP_COUNT; index++)
            if (strstr(ops, op_names[index]))
                ops_mask |= 1 << index;
    }
    const char *classes = getenv("STRESS_CLASS");
    if (classes && *classes) {
        class_mask = 0;
        for (const char *item = classes; *item;) {
            size_t length = strcspn(item, ",");
            for (int index = 0; index < C_COUNT; index++)
                if (strlen(class_names[index]) == length && !strncmp(class_names[index], item, length))
                    class_mask |= 1u << index;
            item += length + (item[length] == ',');
        }
    }
    during_spec = getenv("STRESS_DURING");
    if (during_spec && !*during_spec)
        during_spec = NULL;
    crash_at = env_long("STRESS_CRASH_AT", -1);
    crash_before = (int)env_long("STRESS_CRASH_BEFORE", 0);
    tear = (int)env_long("STRESS_TEAR", 0);
    fail_at = env_long("STRESS_FAIL_AT", -1);
    fail_count = env_long("STRESS_FAIL_COUNT", 1);
    short_io = (int)env_long("STRESS_SHORT", 0);
    const char *probability = getenv("STRESS_FAIL_PROB");
    if (probability && *probability)
        fail_prob = strtod(probability, NULL);
    rng_state ^= (uint64_t)env_long("STRESS_SEED", 1) * 0xBF58476D1CE4E5B9ull;
    const char *errno_list = getenv("STRESS_FAIL_ERRNO");
    for (const char *item = errno_list ? errno_list : ""; *item && fail_errno_count < 8;) {
        size_t length = strcspn(item, ",");
        int value = errno_named(item, length);
        if (value)
            fail_errnos[fail_errno_count++] = value;
        item += length + (item[length] == ',');
    }
    const char *trace = getenv("STRESS_TRACE");
    if (trace && *trace)
        trace_fd = real_open(trace, O_WRONLY | O_CREAT | O_APPEND, 0644);
    count_path = getenv("STRESS_COUNT");
    if (count_path && !*count_path)
        count_path = NULL;
    places_path = getenv("STRESS_PLACES");
    if (places_path && !*places_path)
        places_path = NULL;
    killed_path = getenv("STRESS_KILLED");
    if (killed_path && !*killed_path)
        killed_path = NULL;
    if (count_path || places_path)
        atexit(at_exit);
    initialized = 1;
}

// The harness's store operations: `counts[i]` running operations of the kind named by the i-th
// of the comma separated `names`.
void crashpoint_register(const volatile size_t *counts, const char *names) {
    int count = 0;
    for (const char *item = names; *item && count < MAX_KINDS;) {
        size_t length = strcspn(item, ",");
        size_t take = length < sizeof kind_names[0] - 1 ? length : sizeof kind_names[0] - 1;
        memcpy(kind_names[count], item, take);
        kind_names[count][take] = 0;
        count++;
        item += length + (item[length] == ',');
    }
    kind_count = count;
    if (during_spec) {
        for (const char *item = during_spec; *item;) {
            size_t length = strcspn(item, ",");
            if (length == 4 && !strncmp(item, "idle", 4))
                during_idle = 1;
            for (int index = 0; index < kind_count; index++)
                if (strlen(kind_names[index]) == length && !strncmp(kind_names[index], item, length))
                    during_mask |= 1ull << index;
            item += length + (item[length] == ',');
        }
    }
    __atomic_store_n(&running, counts, __ATOMIC_RELEASE);
    __atomic_store_n(&during_set, 1, __ATOMIC_RELEASE);
}

// Fail nothing more and kill nowhere from now on, returning how many operations were failed.
long crashpoint_disarm(void) {
    __atomic_store_n(&disarmed, 1, __ATOMIC_SEQ_CST);
    return __atomic_load_n(&faults, __ATOMIC_SEQ_CST);
}

// How many operations were failed so far.
long crashpoint_faults(void) { return __atomic_load_n(&faults, __ATOMIC_SEQ_CST); }

static int path_matches(const char *path) {
    return filter && path && strstr(path, filter);
}

static int fd_watched(int fd) {
    return initialized && filter && fd >= 0 && fd < MAX_FDS && watched[fd];
}

static void note_open(int fd) {
    if (!initialized || !filter || fd < 0 || fd >= MAX_FDS)
        return;
    char link[64], target[PATH_MAX];
    snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
    ssize_t length = readlink(link, target, sizeof target - 1);
    if (length <= 0) {
        watched[fd] = 0;
        return;
    }
    target[length] = 0;
    watched[fd] = path_matches(target) ? 1 : 0;
}

static int ends_with(const char *text, size_t length, const char *suffix) {
    size_t suffix_length = strlen(suffix);
    return length >= suffix_length && !memcmp(text + length - suffix_length, suffix, suffix_length);
}

// The kind of store file `path` names.
static int classify(const char *path) {
    if (!path)
        return C_OTHER;
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;
    size_t length = strlen(base);
    const char *pack = strstr(path, "/pack/");
    if (pack && !strchr(pack + 6, '/') && pack[6])
        return C_PACK;
    if (!strncmp(base, "index_", 6)) {
        if (ends_with(base, length, ".new.tmp"))
            return C_NEW_TMP;
        if (ends_with(base, length, ".new"))
            return C_NEW;
        if (ends_with(base, length, ".tmp"))
            return C_BUCKET_TMP;
        return length == 8 ? C_BUCKET : C_OTHER;
    }
    if (!strcmp(base, "level"))
        return C_LEVEL;
    if (!strcmp(base, "level.tmp"))
        return C_LEVEL_TMP;
    if (!strcmp(base, "level.pending"))
        return C_PENDING;
    if (!strcmp(base, "level.pending.tmp"))
        return C_PENDING_TMP;
    if (!strcmp(base, "compact"))
        return C_COMPACT;
    if (!strcmp(base, "compact.tmp"))
        return C_COMPACT_TMP;
    // A directory of the store: `immutable`, `index`, a group, or a group's `pack`.
    if (!strcmp(base, "immutable") || !strcmp(base, "index") || !strcmp(base, "pack") ||
        (length == 2 && strstr(path, "/index/")))
        return C_DIR;
    const char *immutable = strstr(path, "/immutable/");
    if (immutable && !strchr(immutable + 11, '/'))
        return C_ROOT_FILE;
    return C_OTHER;
}

static uint64_t next_random(void) {
    uint64_t z = __atomic_add_fetch(&rng_state, 0x9E3779B97F4A7C15ull, __ATOMIC_RELAXED);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

static void fd_detail(int fd, char *buffer, size_t size) {
    char link[64];
    snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
    ssize_t length = readlink(link, buffer, size - 1);
    buffer[length > 0 ? length : 0] = 0;
}

static int op_index_of(int op) {
    int index = 0;
    while (op > 1) {
        op >>= 1;
        index++;
    }
    return index;
}

// The errors an operation can meet on its own, with nothing outside the store at work: a full disk
// or quota, a failing device or a file system it remounted read-only, memory or descriptors
// running out, and a signal on a file system that lets one interrupt it.
static int natural(int op, int creates, int value) {
    switch (op) {
    case OP_WRITE:
        return value == ENOSPC || value == EDQUOT || value == EIO || value == EROFS || value == EINTR;
    case OP_SYNC:
        return value == ENOSPC || value == EDQUOT || value == EIO;
    case OP_RENAME:
        return value == ENOSPC || value == EDQUOT || value == EIO || value == EROFS;
    case OP_TRUNCATE:
        return value == EIO || value == EROFS || value == EINTR;
    case OP_UNLINK:
        return value == EIO || value == EROFS || value == ENOSPC;
    case OP_OPEN:
        return value == EMFILE || value == ENFILE || value == ENOMEM || value == EIO || value == EINTR ||
               (creates && (value == ENOSPC || value == EDQUOT || value == EROFS));
    case OP_READ:
        return value == EIO || value == EINTR;
    case OP_STAT:
        return value == EIO || value == ENOMEM;
    case OP_LIST:
        return value == EIO || (creates && (value == EMFILE || value == ENFILE || value == ENOMEM));
    case OP_MKDIR:
        return value == ENOSPC || value == EDQUOT || value == EIO || value == EROFS || value == EMLINK;
    }
    return value == EIO;
}

static int fail_errno_for(int op, int creates) {
    for (int index = 0; index < fail_errno_count; index++)
        if (natural(op, creates, fail_errnos[index]))
            return fail_errnos[index];
    return EIO;
}

static __thread const char *crash_op;
static __thread int crash_class;
static __thread uint64_t crash_mask;
static __thread const char *crash_path;

static void die(void) {
    if (trace_fd >= 0)
        real_write(trace_fd, "KILLED\n", 7);
    if (killed_path) {
        int fd = real_open(killed_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) {
            char during[512], line[PATH_MAX + 700];
            format_mask(crash_mask, during, sizeof during);
            int length = snprintf(line, sizeof line, "%s %s %s %s\n", crash_op ? crash_op : "-",
                                  class_names[crash_class], during, crash_path ? crash_path : "-");
            if (length > 0)
                real_write(fd, line, (size_t)length);
            real_close(fd);
        }
    }
    write_count();
    write_places();
    syscall(SYS_kill, getpid(), SIGKILL);
    for (;;)
        pause();
}

// What to do with counted operation `sequence`.
enum action { PROCEED, FAIL, CRASH_BEFORE, CRASH_AFTER, TEAR };

static enum action decide(long sequence) {
    if (__atomic_load_n(&disarmed, __ATOMIC_RELAXED))
        return PROCEED;
    if (crash_at >= 0 && sequence == crash_at) {
        if (tear)
            return TEAR;
        return crash_before ? CRASH_BEFORE : CRASH_AFTER;
    }
    if (fail_at >= 0 && sequence >= fail_at && sequence < fail_at + fail_count)
        return FAIL;
    if (fail_prob > 0 && (double)(next_random() >> 11) / (double)(1ull << 53) < fail_prob)
        return FAIL;
    return PROCEED;
}

static void trace_line(long sequence, const char *name, int class, uint64_t mask, const char *detail, long result) {
    if (trace_fd < 0)
        return;
    char during[512], line[PATH_MAX + 700];
    format_mask(mask, during, sizeof during);
    int length = snprintf(line, sizeof line, "%ld %s %s %s %s %ld\n", sequence, name, class_names[class], during,
                          detail ? detail : "-", result);
    if (length > 0)
        real_write(trace_fd, line, (size_t)length);
}

// Whether an operation of kind `op` on a file of kind `class` is one to count, given which store
// operations run now.
static int counted(int op, int class, uint64_t mask) {
    if (!(ops_mask & op) || !(class_mask & (1u << class)))
        return 0;
    if (!during_spec)
        return 1;
    if (!__atomic_load_n(&during_set, __ATOMIC_ACQUIRE))
        return 0;
    return (mask & during_mask) || (during_idle && !mask);
}

// Count an operation and decide what to do with it. `creates` says whether an open or a listing
// makes something, which decides the errors it can meet. Sets `*error` for a failure.
static enum action begin(int op, const char *name, const char *detail, int creates, int *error) {
    int class = classify(detail);
    uint64_t mask = running_mask();
    if (!counted(op, class, mask))
        return PROCEED;
    long sequence = __atomic_add_fetch(&counter, 1, __ATOMIC_SEQ_CST);
    enum action action = decide(sequence);
    note_place(op_index_of(op), class, mask, 0);
    if (action == FAIL) {
        *error = fail_errno_for(op, creates);
        __atomic_add_fetch(&faults, 1, __ATOMIC_SEQ_CST);
        note_place(op_index_of(op), class, mask, 1);
        trace_line(sequence, name, class, mask, detail, -*error);
    } else {
        trace_line(sequence, name, class, mask, detail, 0);
    }
    if (action == CRASH_BEFORE || action == CRASH_AFTER || action == TEAR) {
        crash_op = op_names[op_index_of(op)];
        crash_class = class;
        crash_mask = mask;
        crash_path = detail;
        if (action == CRASH_BEFORE)
            die();
    }
    return action;
}

static void end(enum action action) {
    if (action == CRASH_AFTER)
        die();
}

static size_t iov_total(const struct iovec *iov, int count) {
    size_t total = 0;
    for (int index = 0; index < count; index++)
        total += iov[index].iov_len;
    return total;
}

// A write or read of `length` bytes about to fail or tear: how much of it to move first.
static size_t partial(size_t length) {
    return length > 1 ? (size_t)(next_random() % (length - 1)) + 1 : 0;
}

#define WRITE_BODY(call, partial_call, length)                                     \
    do {                                                                           \
        if (!fd_watched(fd))                                                       \
            return call;                                                           \
        char detail[PATH_MAX];                                                     \
        fd_detail(fd, detail, sizeof detail);                                      \
        int error = 0;                                                             \
        enum action action = begin(OP_WRITE, __func__, detail, 0, &error);         \
        if (action == FAIL) {                                                      \
            if (short_io && (length) > 1) {                                        \
                size_t part = partial(length);                                     \
                ssize_t done = partial_call;                                       \
                if (done > 0)                                                      \
                    return done;                                                   \
            }                                                                      \
            errno = error;                                                         \
            return -1;                                                             \
        }                                                                          \
        if (action == TEAR) {                                                      \
            size_t part = partial(length);                                         \
            (void)(partial_call);                                                  \
            die();                                                                 \
        }                                                                          \
        ssize_t result = call;                                                     \
        end(action);                                                               \
        return result;                                                             \
    } while (0)

ssize_t write(int fd, const void *buffer, size_t count) {
    WRITE_BODY(real_write(fd, buffer, count), real_write(fd, buffer, part), count);
}

ssize_t pwrite(int fd, const void *buffer, size_t count, off_t offset) {
    WRITE_BODY(real_pwrite(fd, buffer, count, offset), real_pwrite(fd, buffer, part, offset), count);
}

ssize_t pwrite64(int fd, const void *buffer, size_t count, off_t offset) {
    WRITE_BODY(real_pwrite64(fd, buffer, count, offset), real_pwrite64(fd, buffer, part, offset), count);
}

// A vectored write cut to its first `part` bytes, as one plain write of them.
static ssize_t vector_prefix(int fd, const struct iovec *iov, int count, off_t offset, int positional, size_t part) {
    size_t done = 0;
    for (int index = 0; index < count && done < part; index++) {
        size_t take = iov[index].iov_len < part - done ? iov[index].iov_len : part - done;
        ssize_t written = positional ? real_pwrite64(fd, iov[index].iov_base, take, offset + (off_t)done)
                                     : real_write(fd, iov[index].iov_base, take);
        if (written <= 0)
            break;
        done += (size_t)written;
    }
    return (ssize_t)done;
}

ssize_t writev(int fd, const struct iovec *iov, int count) {
    WRITE_BODY(real_writev(fd, iov, count), vector_prefix(fd, iov, count, 0, 0, part), iov_total(iov, count));
}

ssize_t pwritev(int fd, const struct iovec *iov, int count, off_t offset) {
    WRITE_BODY(real_pwritev(fd, iov, count, offset), vector_prefix(fd, iov, count, offset, 1, part),
               iov_total(iov, count));
}

ssize_t pwritev64(int fd, const struct iovec *iov, int count, off_t offset) {
    WRITE_BODY(real_pwritev64(fd, iov, count, offset), vector_prefix(fd, iov, count, offset, 1, part),
               iov_total(iov, count));
}

ssize_t pwritev2(int fd, const struct iovec *iov, int count, off_t offset, int flags) {
    WRITE_BODY(real_pwritev2(fd, iov, count, offset, flags),
               vector_prefix(fd, iov, count, offset, offset >= 0, part), iov_total(iov, count));
}

// A read about to fail: with STRESS_SHORT, it reads and then reports only part of what it read,
// which a caller must take as a short read and continue from.
#define READ_BODY(call)                                                            \
    do {                                                                           \
        if (!fd_watched(fd))                                                       \
            return call;                                                           \
        char detail[PATH_MAX];                                                     \
        fd_detail(fd, detail, sizeof detail);                                      \
        int error = 0;                                                             \
        enum action action = begin(OP_READ, __func__, detail, 0, &error);          \
        if (action == FAIL) {                                                      \
            if (short_io) {                                                        \
                ssize_t done = call;                                               \
                if (done > 1)                                                      \
                    return (ssize_t)partial((size_t)done);                         \
                if (done >= 0)                                                     \
                    return done;                                                   \
            }                                                                      \
            errno = error;                                                         \
            return -1;                                                             \
        }                                                                          \
        if (action == TEAR)                                                        \
            die();                                                                 \
        ssize_t result = call;                                                     \
        end(action);                                                               \
        return result;                                                             \
    } while (0)

ssize_t read(int fd, void *buffer, size_t count) { READ_BODY(real_read(fd, buffer, count)); }
ssize_t pread(int fd, void *buffer, size_t count, off_t offset) { READ_BODY(real_pread(fd, buffer, count, offset)); }
ssize_t pread64(int fd, void *buffer, size_t count, off_t offset) {
    READ_BODY(real_pread64(fd, buffer, count, offset));
}
ssize_t readv(int fd, const struct iovec *iov, int count) { READ_BODY(real_readv(fd, iov, count)); }
ssize_t preadv(int fd, const struct iovec *iov, int count, off_t offset) {
    READ_BODY(real_preadv(fd, iov, count, offset));
}
ssize_t preadv64(int fd, const struct iovec *iov, int count, off_t offset) {
    READ_BODY(real_preadv64(fd, iov, count, offset));
}
ssize_t preadv2(int fd, const struct iovec *iov, int count, off_t offset, int flags) {
    READ_BODY(real_preadv2(fd, iov, count, offset, flags));
}

#define FD_BODY(op, call)                                                          \
    do {                                                                           \
        if (!fd_watched(fd))                                                       \
            return call;                                                           \
        char detail[PATH_MAX];                                                     \
        fd_detail(fd, detail, sizeof detail);                                      \
        int error = 0;                                                             \
        enum action action = begin(op, __func__, detail, 0, &error);               \
        if (action == FAIL) {                                                      \
            errno = error;                                                         \
            return -1;                                                             \
        }                                                                          \
        if (action == TEAR)                                                        \
            die();                                                                 \
        int result = call;                                                         \
        end(action);                                                               \
        return result;                                                             \
    } while (0)

int fsync(int fd) { FD_BODY(OP_SYNC, real_fsync(fd)); }
int fdatasync(int fd) { FD_BODY(OP_SYNC, real_fdatasync(fd)); }
int ftruncate(int fd, off_t length) { FD_BODY(OP_TRUNCATE, real_ftruncate(fd, length)); }
int ftruncate64(int fd, off_t length) { FD_BODY(OP_TRUNCATE, real_ftruncate64(fd, length)); }

#define PATH_BODY(op, matched, detail, creates, call)                              \
    do {                                                                           \
        if (!initialized || !(matched))                                            \
            return call;                                                           \
        int error = 0;                                                             \
        enum action action = begin(op, __func__, detail, creates, &error);         \
        if (action == FAIL) {                                                      \
            errno = error;                                                         \
            return -1;                                                             \
        }                                                                          \
        if (action == TEAR)                                                        \
            die();                                                                 \
        int result = call;                                                         \
        end(action);                                                               \
        return result;                                                             \
    } while (0)

// A rename is classed by the file it moves, and named in the trace by both.
int rename(const char *from, const char *to) {
    PATH_BODY(OP_RENAME, path_matches(from) || path_matches(to), from, 0, real_rename(from, to));
}

int renameat(int from_dir, const char *from, int to_dir, const char *to) {
    PATH_BODY(OP_RENAME, path_matches(from) || path_matches(to), from, 0, real_renameat(from_dir, from, to_dir, to));
}

int renameat2(int from_dir, const char *from, int to_dir, const char *to, unsigned int flags) {
    PATH_BODY(OP_RENAME, path_matches(from) || path_matches(to), from, 0,
              real_renameat2(from_dir, from, to_dir, to, flags));
}

int unlink(const char *path) { PATH_BODY(OP_UNLINK, path_matches(path), path, 0, real_unlink(path)); }

int unlinkat(int dir, const char *path, int flags) {
    PATH_BODY(OP_UNLINK, path_matches(path), path, 0, real_unlinkat(dir, path, flags));
}

int mkdir(const char *path, mode_t mode) { PATH_BODY(OP_MKDIR, path_matches(path), path, 1, real_mkdir(path, mode)); }

int mkdirat(int dir, const char *path, mode_t mode) {
    PATH_BODY(OP_MKDIR, path_matches(path), path, 1, real_mkdirat(dir, path, mode));
}

// Stats by path, or by descriptor with an empty path.
static int stat_matched(int dir, const char *path, int flags) {
    if (path && !*path && (flags & AT_EMPTY_PATH))
        return fd_watched(dir);
    return path_matches(path);
}

static void stat_detail(int dir, const char *path, char *buffer, size_t size) {
    if (path && *path)
        snprintf(buffer, size, "%s", path);
    else
        fd_detail(dir, buffer, size);
}

#define STAT_BODY(dir, path, flags, call)                                          \
    do {                                                                           \
        if (!initialized || !stat_matched(dir, path, flags))                       \
            return call;                                                           \
        char detail[PATH_MAX];                                                     \
        stat_detail(dir, path, detail, sizeof detail);                             \
        int error = 0;                                                             \
        enum action action = begin(OP_STAT, __func__, detail, 0, &error);          \
        if (action == FAIL) {                                                      \
            errno = error;                                                         \
            return -1;                                                             \
        }                                                                          \
        if (action == TEAR)                                                        \
            die();                                                                 \
        int result = call;                                                         \
        end(action);                                                               \
        return result;                                                             \
    } while (0)

int statx(int dir, const char *path, int flags, unsigned int mask, struct statx *buffer) {
    STAT_BODY(dir, path, flags, real_statx(dir, path, flags, mask, buffer));
}

static int fstatat_fallback(int dir, const char *path, void *buffer, int flags) {
    return (int)syscall(SYS_newfstatat, dir, path, buffer, flags);
}

int stat(const char *path, struct stat *buffer) {
    STAT_BODY(AT_FDCWD, path, 0, real_stat ? real_stat(path, buffer) : fstatat_fallback(AT_FDCWD, path, buffer, 0));
}
int stat64(const char *path, struct stat64 *buffer) {
    STAT_BODY(AT_FDCWD, path, 0,
              real_stat64 ? real_stat64(path, buffer) : fstatat_fallback(AT_FDCWD, path, buffer, 0));
}
int lstat(const char *path, struct stat *buffer) {
    STAT_BODY(AT_FDCWD, path, 0,
              real_lstat ? real_lstat(path, buffer)
                         : fstatat_fallback(AT_FDCWD, path, buffer, AT_SYMLINK_NOFOLLOW));
}
int lstat64(const char *path, struct stat64 *buffer) {
    STAT_BODY(AT_FDCWD, path, 0,
              real_lstat64 ? real_lstat64(path, buffer)
                           : fstatat_fallback(AT_FDCWD, path, buffer, AT_SYMLINK_NOFOLLOW));
}
int fstat(int fd, struct stat *buffer) {
    STAT_BODY(fd, "", AT_EMPTY_PATH, real_fstat ? real_fstat(fd, buffer) : (int)syscall(SYS_fstat, fd, buffer));
}
int fstat64(int fd, struct stat64 *buffer) {
    STAT_BODY(fd, "", AT_EMPTY_PATH,
              real_fstat64 ? real_fstat64(fd, buffer) : (int)syscall(SYS_fstat, fd, buffer));
}
int fstatat(int dir, const char *path, struct stat *buffer, int flags) {
    STAT_BODY(dir, path, flags,
              real_fstatat ? real_fstatat(dir, path, buffer, flags) : fstatat_fallback(dir, path, buffer, flags));
}
int fstatat64(int dir, const char *path, struct stat64 *buffer, int flags) {
    STAT_BODY(dir, path, flags,
              real_fstatat64 ? real_fstatat64(dir, path, buffer, flags)
                             : fstatat_fallback(dir, path, buffer, flags));
}

// Listing a directory: opening it, and each read of its entries, which fails as the end does,
// with errno set.
DIR *opendir(const char *path) {
    if (!initialized || !path_matches(path)) {
        DIR *dir = real_opendir(path);
        if (dir)
            note_open(dirfd(dir));
        return dir;
    }
    int error = 0;
    enum action action = begin(OP_LIST, __func__, path, 1, &error);
    if (action == FAIL) {
        errno = error;
        return NULL;
    }
    if (action == TEAR)
        die();
    DIR *dir = real_opendir(path);
    if (dir)
        note_open(dirfd(dir));
    end(action);
    return dir;
}

#define READDIR_BODY(type, call)                                                   \
    do {                                                                           \
        int fd = dirfd(dir);                                                       \
        if (!fd_watched(fd))                                                       \
            return call;                                                           \
        char detail[PATH_MAX];                                                     \
        fd_detail(fd, detail, sizeof detail);                                      \
        int error = 0;                                                             \
        enum action action = begin(OP_LIST, __func__, detail, 0, &error);          \
        if (action == FAIL) {                                                      \
            errno = error;                                                         \
            return NULL;                                                           \
        }                                                                          \
        if (action == TEAR)                                                        \
            die();                                                                 \
        type *entry = call;                                                        \
        end(action);                                                               \
        return entry;                                                              \
    } while (0)

struct dirent *readdir(DIR *dir) { READDIR_BODY(struct dirent, real_readdir(dir)); }
struct dirent64 *readdir64(DIR *dir) { READDIR_BODY(struct dirent64, real_readdir64(dir)); }

static int open_common(int (*real_call)(const char *, int, ...), const char *name, const char *path, int flags,
                       mode_t mode) {
    if (initialized && path_matches(path)) {
        int error = 0;
        enum action action = begin(OP_OPEN, name, path, (flags & O_CREAT) != 0, &error);
        if (action == FAIL) {
            errno = error;
            return -1;
        }
        if (action == TEAR)
            die();
        int fd = real_call(path, flags, mode);
        note_open(fd);
        end(action);
        return fd;
    }
    int fd = real_call(path, flags, mode);
    note_open(fd);
    return fd;
}

static int openat_common(int (*real_call)(int, const char *, int, ...), const char *name, int dir, const char *path,
                         int flags, mode_t mode) {
    if (initialized && path_matches(path)) {
        int error = 0;
        enum action action = begin(OP_OPEN, name, path, (flags & O_CREAT) != 0, &error);
        if (action == FAIL) {
            errno = error;
            return -1;
        }
        if (action == TEAR)
            die();
        int fd = real_call(dir, path, flags, mode);
        note_open(fd);
        end(action);
        return fd;
    }
    int fd = real_call(dir, path, flags, mode);
    note_open(fd);
    return fd;
}

#define MODE_ARG(flags)                                                            \
    mode_t mode = 0;                                                               \
    if ((flags) & (O_CREAT | O_TMPFILE)) {                                         \
        va_list arguments;                                                         \
        va_start(arguments, flags);                                                \
        mode = (mode_t)va_arg(arguments, int);                                     \
        va_end(arguments);                                                         \
    }

int open(const char *path, int flags, ...) {
    MODE_ARG(flags);
    return open_common(real_open, "open", path, flags, mode);
}

int open64(const char *path, int flags, ...) {
    MODE_ARG(flags);
    return open_common(real_open64, "open64", path, flags, mode);
}

int openat(int dir, const char *path, int flags, ...) {
    MODE_ARG(flags);
    return openat_common(real_openat, "openat", dir, path, flags, mode);
}

int openat64(int dir, const char *path, int flags, ...) {
    MODE_ARG(flags);
    return openat_common(real_openat64, "openat64", dir, path, flags, mode);
}

int close(int fd) {
    if (fd >= 0 && fd < MAX_FDS)
        watched[fd] = 0;
    return real_close(fd);
}
