#include "../hashtable.h"
#include "../volatile_set.h"
#include "../listpack.h"
#include "../server.h"

#include "test_help.h"

#include <stdio.h>
#include <limits.h>
#include <string.h>
#include <math.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>


int test_basic(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);
    char *entry1 = "hello";
    char *entry2 = "world";

    // Insert first entry
    TEST_ASSERT(volatileSetAddEntry(vset, entry1, 10000000) == 1);

    // Compute the bucket key
    long long bucket_ts = 10000000 / 60000 * 60000;
    long long be_ts = htonu64(bucket_ts);
    unsigned char key[sizeof(be_ts)];
    memcpy(key, &be_ts, sizeof(be_ts));

    // Lookup bucket
    void *raw = NULL;
    int found = raxFind(vset->expiry_buckets, key, sizeof(key), &raw);
    TEST_ASSERT(found == 1);
    vsetBucket *bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_SINGLE);
    TEST_ASSERT(bucket->data.single == entry1);

    // Insert second entry (should trigger listpack upgrade)
    TEST_ASSERT(volatileSetAddEntry(vset, entry2, 10000000) == 1);

    found = raxFind(vset->expiry_buckets, key, sizeof(key), &raw);
    TEST_ASSERT(found == 1);
    bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_LISTPACK);

    // Validate contents of listpack
    TEST_ASSERT(lpLength(bucket->data.listpack) == 2);
    unsigned char *p = lpFirst(bucket->data.listpack);

    unsigned int slen;
    long long lval;

    lpGetValue(p, &slen, &lval);

    // Validate pointer length first
    TEST_ASSERT(strcmp((char *)lval, "hello") == 0);
    p = lpNext(bucket->data.listpack, p);
    lpGetValue(p, &slen, &lval);
    TEST_ASSERT(strcmp((char *)lval, "world") == 0);

    lpFree(bucket->data.listpack);
    zfree(bucket);

    freeVolatileSet(vset);
    return 0;
}

sds createSds(char *str) {
    return sdscat(sdsempty(), str);
}

#define VSET_BUCKET_KEY_LEN 32
int test_update_entry(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);

    long long expiry1 = 10000000;         // initial expiry
    long long expiry2 = 10000000 + 60000; // moved to next bucket

    entry *entry = entryCreate(createSds("field1"), createSds("value1"), expiry1);

    // Add entry to first bucket
    TEST_ASSERT(volatileSetAddEntry(vset, entry, expiry1) == 1);

    // Build key for first bucket
    unsigned char key1[VSET_BUCKET_KEY_LEN] = {0};
    size_t key1_len = encodeExpiryBucketKey(key1, expiry1);

    void *raw = NULL;
    int found = raxFind(vset->expiry_buckets, key1, key1_len, &raw);
    TEST_ASSERT(found == 1);
    vsetBucket *bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_SINGLE);
    TEST_ASSERT(bucket->data.single == entry);

    // Now update the entry to a new expiry (should move it to a new bucket)
    TEST_ASSERT(volatileSetUpdateEntry(vset, entry, entry, expiry1, expiry2) == 1);

    // Old bucket should no longer exist
    TEST_ASSERT(raxFind(vset->expiry_buckets, key1, sizeof(key1), NULL) == 0);

    // New bucket should contain the entry
    unsigned char key2[VSET_BUCKET_KEY_LEN] = {0};
    size_t key2_len = encodeExpiryBucketKey(key2, expiry2);
    found = raxFind(vset->expiry_buckets, key2, key2_len, &raw);
    TEST_ASSERT(found == 1);
    bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_SINGLE);
    TEST_ASSERT(bucket->data.single == entry);

    freeVolatileSet(vset);
    return 0;
}

int test_hashtable(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);

    // volatile int stop = 1;
    // while (stop);
    // zmalloc_usable(28,NULL);


    char entries[129][32];
    entry *hashEntries[129];
    for (int i = 0; i < 129; i++) {
        snprintf(entries[i], sizeof(entries[i]), "entry_%d", i);
        sds field = sdsempty();
        field = sdscat(field, entries[i]);
        sds val = sdsempty();
        val = sdscat(val, "field");
        entry *ptr = entryCreate(field, val, 100);
        hashEntries[i] = ptr;
    }

    int expiry = 10000000;
    for (int i = 0; i < 129; i++) {
        TEST_ASSERT(volatileSetAddEntry(vset, hashEntries[i], expiry) == 1);
    }

    // Compute the bucket key
    long long bucket_ts = 10000000 / 60000 * 60000;
    long long be_ts = htonu64(bucket_ts);
    unsigned char key[sizeof(be_ts)];
    memcpy(key, &be_ts, sizeof(be_ts));

    void *raw = NULL;
    int found = raxFind(vset->expiry_buckets, key, sizeof(key), &raw);
    TEST_ASSERT(found == 1);
    vsetBucket *bucket = raw;

    TEST_ASSERT(bucket->type == VSET_BUCKET_HT);

    for (int i = 0; i < 129; i++) {
        volatileSetRemoveEntry(vset, hashEntries[i], expiry);
    }

    freeVolatileSet(vset);
    return 0;
}


int test_promotion(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);
    long long expiry = 12345678;

    entry *entries[130];

    // Insert 1st entry → SINGLE
    entries[0] = entryCreate(createSds("field_0"), createSds("val_0"), expiry);
    TEST_ASSERT(volatileSetAddEntry(vset, entries[0], expiry) == 1);

    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    size_t key_len = encodeExpiryBucketKey(key, expiry);

    void *raw = NULL;
    TEST_ASSERT(raxFind(vset->expiry_buckets, key, key_len, &raw) == 1);
    vsetBucket *bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_SINGLE);
    TEST_ASSERT(bucket->data.single == entries[0]);

    // Insert entries 1..127 → should be LISTPACK (128 total)
    for (int i = 1; i <= 127; i++) {
        char fname[32], fval[32];
        snprintf(fname, sizeof(fname), "field_%d", i);
        snprintf(fval, sizeof(fval), "val_%d", i);
        entries[i] = entryCreate(createSds(fname), createSds(fval), expiry);
        TEST_ASSERT(volatileSetAddEntry(vset, entries[i], expiry) == 1);
    }

    TEST_ASSERT(raxFind(vset->expiry_buckets, key, key_len, &raw) == 1);
    bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_LISTPACK);
    TEST_ASSERT(lpLength(bucket->data.listpack) == 128);

    // Insert 129th → triggers promotion to HT
    entries[128] = entryCreate(createSds("field_128"), createSds("val_128"), expiry);
    TEST_ASSERT(volatileSetAddEntry(vset, entries[128], expiry) == 1);

    TEST_ASSERT(raxFind(vset->expiry_buckets, key, key_len, &raw) == 1);
    bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_HT);

    for (int i = 0; i <= 128; i++) {
        void *found;
        TEST_ASSERT(hashtableFind(bucket->data.hashtable, entries[i], &found));
    }

    freeVolatileSet(vset);
    return 0;
}

int test_demotion_ht_to_single(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);
    long long expiry = 12345678;

    entry *entries[130];

    // Insert 130 entries to reach HT
    for (int i = 0; i < 130; i++) {
        char fname[32], fval[32];
        snprintf(fname, sizeof(fname), "field_%d", i);
        snprintf(fval, sizeof(fval), "val_%d", i);
        entries[i] = entryCreate(createSds(fname), createSds(fval), expiry);
        TEST_ASSERT(volatileSetAddEntry(vset, entries[i], expiry) == 1);
    }

    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    size_t key_len = encodeExpiryBucketKey(key, expiry);

    void *raw = NULL;
    TEST_ASSERT(raxFind(vset->expiry_buckets, key, key_len, &raw) == 1);
    vsetBucket *bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_HT);

    // Remove down to 2 entries
    for (int i = 0; i < 128; i++) {
        TEST_ASSERT(volatileSetRemoveEntry(vset, entries[i], expiry) == 1);
    }

    // Still HT
    TEST_ASSERT(raxFind(vset->expiry_buckets, key, key_len, &raw) == 1);
    bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_HT);

    // Only two entries left
    TEST_ASSERT(hashtableSize(bucket->data.hashtable) == 2);

    // Remove one more → down to 1
    TEST_ASSERT(volatileSetRemoveEntry(vset, entries[128], expiry) == 1);

    // Should now demote to SINGLE
    TEST_ASSERT(raxFind(vset->expiry_buckets, key, key_len, &raw) == 1);
    bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_SINGLE);
    TEST_ASSERT(bucket->data.single == entries[129]);

    freeVolatileSet(vset);
    return 0;
}

int test_update_same_bucket_noop(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);
    long long expiry = 10000000;

    entry *e = entryCreate(createSds("f"), createSds("v"), expiry);
    TEST_ASSERT(volatileSetAddEntry(vset, e, expiry) == 1);

    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    size_t key_len = encodeExpiryBucketKey(key, expiry);

    void *raw = NULL;
    TEST_ASSERT(raxFind(vset->expiry_buckets, key, key_len, &raw) == 1);
    vsetBucket *bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_SINGLE);
    TEST_ASSERT(bucket->data.single == e);

    // Now call update with same pointer, same expiry
    TEST_ASSERT(volatileSetUpdateEntry(vset, e, e, expiry, expiry) == 1);

    // Bucket should still be SINGLE with same pointer
    TEST_ASSERT(raxFind(vset->expiry_buckets, key, key_len, &raw) == 1);
    bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_SINGLE);
    TEST_ASSERT(bucket->data.single == e);

    freeVolatileSet(vset);
    return 0;
}


int test_update_entry_within_same_bucket(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);
    long long expiry = 10000000;

    entry *entry1 = entryCreate(createSds("f1"), createSds("v1"), expiry);
    entry *entry2 = entryCreate(createSds("f2"), createSds("v2"), expiry);

    // Add entry1
    TEST_ASSERT(volatileSetAddEntry(vset, entry1, expiry) == 1);

    // Update entry1 → entry2 (same expiry)
    TEST_ASSERT(volatileSetUpdateEntry(vset, entry1, entry2, expiry, expiry) == 1);

    // Verify bucket has only entry2
    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    size_t key_len = encodeExpiryBucketKey(key, expiry);
    void *raw = NULL;
    TEST_ASSERT(raxFind(vset->expiry_buckets, key, key_len, &raw) == 1);
    vsetBucket *bucket = raw;

    TEST_ASSERT(bucket->type == VSET_BUCKET_SINGLE);
    TEST_ASSERT(bucket->data.single == entry2);

    freeVolatileSet(vset);
    return 0;
}

int test_update_entry_across_buckets(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);

    long long expiry1 = 10000000;
    long long expiry2 = expiry1 + VOLATILESET_BUCKET_GRANULARITY; // next bucket

    entry *entry1 = entryCreate(createSds("f1"), createSds("v1"), expiry1);
    entry *entry2 = entryCreate(createSds("f2"), createSds("v2"), expiry2);

    // Add entry1 to first bucket
    TEST_ASSERT(volatileSetAddEntry(vset, entry1, expiry1) == 1);

    // Update entry1 → entry2 with different expiry
    TEST_ASSERT(volatileSetUpdateEntry(vset, entry1, entry2, expiry1, expiry2) == 1);

    // Old bucket should be gone
    unsigned char key1[VSET_BUCKET_KEY_LEN] = {0};
    size_t key1_len = encodeExpiryBucketKey(key1, expiry1);
    TEST_ASSERT(raxFind(vset->expiry_buckets, key1, key1_len, NULL) == 0);

    // New bucket should contain entry2
    unsigned char key2[VSET_BUCKET_KEY_LEN] = {0};
    size_t key2_len = encodeExpiryBucketKey(key2, expiry2);
    void *raw = NULL;
    TEST_ASSERT(raxFind(vset->expiry_buckets, key2, key2_len, &raw) == 1);
    vsetBucket *bucket = raw;

    TEST_ASSERT(bucket->type == VSET_BUCKET_SINGLE);
    TEST_ASSERT(bucket->data.single == entry2);

    freeVolatileSet(vset);
    return 0;
}


static int run_crash_test(void (*crashy)(void)) {
    pid_t pid = fork();
    if (pid == 0) {
        crashy();
        exit(0); // should not reach
    }

    int status;
    waitpid(pid, &status, 0);

    return WIFEXITED(status) && WEXITSTATUS(status) != 0;
}

static void crash_duplicate_in_single(void) {
    volatile_set *vset = createVolatileSet(NULL);
    long long expiry = 10000000;
    entry *e = entryCreate(createSds("f"), createSds("v"), expiry);
    volatileSetAddEntry(vset, e, expiry);
    volatileSetAddEntry(vset, e, expiry); // triggers assert
}

static void crash_duplicate_in_listpack(void) {
    volatile_set *vset = createVolatileSet(NULL);
    long long expiry = 10000000;
    entry *e1 = entryCreate(createSds("f1"), createSds("v1"), expiry);
    entry *e2 = entryCreate(createSds("f2"), createSds("v2"), expiry);
    volatileSetAddEntry(vset, e1, expiry);
    volatileSetAddEntry(vset, e2, expiry); // promotes to LISTPACK
    volatileSetAddEntry(vset, e1, expiry); // triggers assert
}

static void crash_duplicate_in_hashtable(void) {
    volatile_set *vset = createVolatileSet(NULL);
    long long expiry = 10000000;

    entry *e0 = NULL;
    for (int i = 0; i < 129; i++) {
        char f[32], v[32];
        snprintf(f, sizeof(f), "f%d", i);
        snprintf(v, sizeof(v), "v%d", i);
        entry *e = entryCreate(createSds(f), createSds(v), expiry);
        if (i == 0) e0 = e;
        volatileSetAddEntry(vset, e, expiry);
    }

    volatileSetAddEntry(vset, e0, expiry); // triggers assert
}

// int test_duplicate_insert_should_crash(int argc, char **argv, int flags) {
//     UNUSED(argc); UNUSED(argv); UNUSED(flags);
//
//     TEST_ASSERT(run_crash_test(crash_duplicate_in_single));
//     TEST_ASSERT(run_crash_test(crash_duplicate_in_listpack));
//     TEST_ASSERT(run_crash_test(crash_duplicate_in_hashtable));
//
//     return 0;
// }

int test_bucket_order_by_expiry(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);

    long long expiries[] = {180000, 60000, 120000}; // intentionally unordered
    long long expected[] = {60000, 120000, 180000};

    for (int i = 0; i < 3; i++) {
        char f[16], v[16];
        snprintf(f, sizeof(f), "f%lld", expiries[i]);
        snprintf(v, sizeof(v), "v%lld", expiries[i]);
        entry *e = entryCreate(createSds(f), createSds(v), expiries[i]);
        TEST_ASSERT(volatileSetAddEntry(vset, e, expiries[i]) == 1);
    }

    raxIterator ri;
    raxStart(&ri, vset->expiry_buckets);
    raxSeek(&ri, ">=", NULL, 0);

    int i = 0;
    while (raxNext(&ri)) {
        long long ts;
        memcpy(&ts, ri.key, sizeof(ts));
        ts = ntohu64(ts); // convert back from BE
        TEST_ASSERT(ts == expected[i]);
        i++;
    }
    TEST_ASSERT(i == 3); // Ensure we saw all buckets

    raxStop(&ri);
    freeVolatileSet(vset);
    return 0;
}

int test_bucket_lookup_strictly_below(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);
    long long buckets[] = {60000, 120000, 180000};
    entry *e[3];

    for (int i = 0; i < 3; i++) {
        char f[16], v[16];
        snprintf(f, sizeof(f), "f%lld", buckets[i]);
        snprintf(v, sizeof(v), "v%lld", buckets[i]);
        e[i] = entryCreate(createSds(f), createSds(v), buckets[i]);
        TEST_ASSERT(volatileSetAddEntry(vset, e[i], buckets[i]) == 1);
    }

    // Try to find the bucket just below or equal to 119000 → should get 60000
    long long lookup_ts = 119000;
    unsigned char lookup_key[VSET_BUCKET_KEY_LEN] = {0};
    encodeExpiryBucketKey(lookup_key, lookup_ts);

    raxIterator ri;
    raxStart(&ri, vset->expiry_buckets);
    raxSeek(&ri, ">=", lookup_key, sizeof(long long));

    if (!raxPrev(&ri)) {
        TEST_ASSERT(!"Expected to find bucket below");
    } else {
        long long ts;
        memcpy(&ts, ri.key, sizeof(ts));
        ts = ntohu64(ts);

        TEST_ASSERT(ts == 60000);
        // TEST_ASSERT(ri.data == e[0]); // matches entry at 60000
    }

    raxStop(&ri);
    freeVolatileSet(vset);
    return 0;
}


int test_iterator_basic(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);
    long long expiry = 10000000;

    entry *entries[130];
    for (int i = 0; i < 130; i++) {
        char f[32], v[32];
        snprintf(f, sizeof(f), "f%d", i);
        snprintf(v, sizeof(v), "v%d", i);
        entries[i] = entryCreate(createSds(f), createSds(v), expiry);
        TEST_ASSERT(volatileSetAddEntry(vset, entries[i], expiry) == 1);
    }

    int seen[130] = {0};
    volatileSetIterator it;
    volatileSetStart(vset, &it);
    void *ptr;

    int count = 0;
    while (volatileSetNext(&it, &ptr)) {
        for (int i = 0; i < 130; i++) {
            if (entries[i] == ptr) {
                TEST_ASSERT(seen[i] == 0); // no duplicates
                seen[i] = 1;
                count++;
                break;
            }
        }
    }
    volatileSetReset(&it);

    TEST_ASSERT(count == 130);
    for (int i = 0; i < 130; i++)
        TEST_ASSERT(seen[i] == 1);

    freeVolatileSet(vset);
    return 0;
}

int test_iterator_listpack(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);
    long long expiry = 12345678;

    entry *entries[50];
    for (int i = 0; i < 50; i++) {
        char f[32], v[32];
        snprintf(f, sizeof(f), "f%d", i);
        snprintf(v, sizeof(v), "v%d", i);
        entries[i] = entryCreate(createSds(f), createSds(v), expiry);
        TEST_ASSERT(volatileSetAddEntry(vset, entries[i], expiry) == 1);
    }

    // Check the bucket type is LISTPACK
    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    size_t key_len = encodeExpiryBucketKey(key, expiry);
    void *raw = NULL;
    TEST_ASSERT(raxFind(vset->expiry_buckets, key, key_len, &raw) == 1);
    vsetBucket *bucket = raw;
    TEST_ASSERT(bucket->type == VSET_BUCKET_LISTPACK);
    TEST_ASSERT(lpLength(bucket->data.listpack) == 50);

    // Iterate
    int seen[50] = {0};
    volatileSetIterator it;
    volatileSetStart(vset, &it);
    void *ptr;
    int count = 0;

    while (volatileSetNext(&it, &ptr)) {
        for (int i = 0; i < 50; i++) {
            if (entries[i] == ptr) {
                TEST_ASSERT(seen[i] == 0);
                seen[i] = 1;
                count++;
                break;
            }
        }
    }

    volatileSetReset(&it);

    TEST_ASSERT(count == 50);
    for (int i = 0; i < 50; i++)
        TEST_ASSERT(seen[i] == 1);

    freeVolatileSet(vset);
    return 0;
}

int test_iterator_mixed_buckets(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    volatile_set *vset = createVolatileSet(NULL);
    long long expiry1 = 10000000;        // SINGLE
    long long expiry2 = expiry1 + 60000; // LISTPACK
    long long expiry3 = expiry2 + 60000; // HT

    entry *e_single = entryCreate(createSds("s"), createSds("v"), expiry1);
    TEST_ASSERT(volatileSetAddEntry(vset, e_single, expiry1) == 1);

    entry *e_listpack[50];
    for (int i = 0; i < 50; i++) {
        char f[32], v[32];
        snprintf(f, sizeof(f), "lp_f%d", i);
        snprintf(v, sizeof(v), "lp_v%d", i);
        e_listpack[i] = entryCreate(createSds(f), createSds(v), expiry2);
        TEST_ASSERT(volatileSetAddEntry(vset, e_listpack[i], expiry2) == 1);
    }

    entry *e_ht[130];
    for (int i = 0; i < 130; i++) {
        char f[32], v[32];
        snprintf(f, sizeof(f), "ht_f%d", i);
        snprintf(v, sizeof(v), "ht_v%d", i);
        e_ht[i] = entryCreate(createSds(f), createSds(v), expiry3);
        TEST_ASSERT(volatileSetAddEntry(vset, e_ht[i], expiry3) == 1);
    }

    int found_single = 0;
    int seen_listpack[50] = {0};
    int seen_ht[130] = {0};

    volatileSetIterator it;
    volatileSetStart(vset, &it);
    void *ptr;
    int count = 0;

    int stop = 0;
    while (stop);

    while (volatileSetNext(&it, &ptr)) {
        if (ptr == e_single) {
            TEST_ASSERT(found_single == 0);
            found_single = 1;
            goto seen;
        }
        for (int i = 0; i < 50; i++) {
            if (ptr == e_listpack[i]) {
                TEST_ASSERT(seen_listpack[i] == 0);
                seen_listpack[i] = 1;
                goto seen;
            }
        }
        for (int i = 0; i < 130; i++) {
            if (ptr == e_ht[i]) {
                TEST_ASSERT(seen_ht[i] == 0);
                seen_ht[i] = 1;
                goto seen;
            }
        }
        TEST_ASSERT(!"Unknown entry found");

    seen:
        count++;
    }
    volatileSetReset(&it);

    TEST_ASSERT(count == 1 + 50 + 130);
    TEST_ASSERT(found_single == 1);
    for (int i = 0; i < 50; i++) TEST_ASSERT(seen_listpack[i] == 1);
    for (int i = 0; i < 130; i++) TEST_ASSERT(seen_ht[i] == 1);

    freeVolatileSet(vset);
    return 0;
}
