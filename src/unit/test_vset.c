#include "../vset.h"
#include "../entry.h"
#include "test_help.h"
#include "../zmalloc.h"
#include <stdio.h>
#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <time.h>


typedef entry mock_entry;

static mock_entry *mockCreateEntry(const char *keystr, long long expiry) {
    sds field = sdsnew(keystr);
    mock_entry *e = entryCreate(field, sdsnew("value"), expiry);
    sdsfree(field);
    return e;
}

static mock_entry *mockEntryUpdate(mock_entry *entry, long long expiry) {
    return entryUpdate(entry, NULL, expiry);
}

static long long mockGetExpiry(const void *entry) {
    return entryGetExpiry(entry);
}

static void mockFreeEntry(void *entry) {
    // printf("mockFreeEntry: %p\n", entry);
    entryFree(entry);
}

int test_vset_add_and_iterate(int argc, char **argv, int flags) {
    (void)argc;
    (void)argv;
    (void)flags;

    vset *set = createVolatileSet();
    TEST_ASSERT(set != NULL);

    mock_entry *e1 = mockCreateEntry("item1", 123);
    mock_entry *e2 = mockCreateEntry("item2", 456);

    TEST_ASSERT(vsetAddEntry(set, mockGetExpiry, e1, mockGetExpiry(e1)));
    TEST_ASSERT(vsetAddEntry(set, mockGetExpiry, e2, mockGetExpiry(e2)));

    TEST_ASSERT(!vsetIsEmpty(set));

    vsetIterator it;
    vsetStart(set, &it);

    void *entry;
    int count = 0;
    while (vsetNext(&it, &entry)) {
        TEST_EXPECT(entry != NULL);
        count++;
    }

    TEST_ASSERT(count == 2);

    vsetStop(&it);
    freeVolatileSet(set);
    mockFreeEntry(e1);
    mockFreeEntry(e2);

    TEST_PRINT_INFO("Test passed with %d expects", failed_expects);
    return 0;
}

int test_vset_large_batch_same_expiry(int argc, char **argv, int flags) {
    (void)argc;
    (void)argv;
    (void)flags;

    vset *set = createVolatileSet();
    TEST_ASSERT(set != NULL);

    const long long expiry_time = 1000LL;
    const int total_entries = 200;

    // Allocate and add 200 entries with same expiry
    mock_entry **entries = zmalloc(sizeof(mock_entry *) * total_entries);
    TEST_ASSERT(entries != NULL);

    for (int i = 0; i < total_entries; i++) {
        char key_buf[32];
        snprintf(key_buf, sizeof(key_buf), "entry_%d", i);
        entries[i] = mockCreateEntry(key_buf, expiry_time);
        TEST_ASSERT(vsetAddEntry(set, mockGetExpiry, entries[i], expiry_time));
    }

    // Verify set is not empty
    TEST_ASSERT(!vsetIsEmpty(set));

    // Iterate all entries and count them
    vsetIterator it;
    vsetStart(set, &it);

    void *entry;
    int count = 0;
    while (vsetNext(&it, &entry)) {
        TEST_EXPECT(entry != NULL);
        count++;
    }
    TEST_ASSERT(count == total_entries);

    // Cleanup
    vsetStop(&it);
    freeVolatileSet(set);

    for (int i = 0; i < total_entries; i++) {
        mockFreeEntry(entries[i]);
    }
    zfree(entries);

    TEST_PRINT_INFO("Inserted and iterated %d entries with same expiry", total_entries);
    return 0;
}

int test_vset_iterate_multiple_expiries(int argc, char **argv, int flags) {
    (void)argc;
    (void)argv;
    (void)flags;
    const unsigned int total_entries = 5;

    vset *set = createVolatileSet();
    TEST_ASSERT(set != NULL);

    // Prepare entries with mixed expiry times, some duplicates
    mock_entry *entries[total_entries];

    // Initialize keys
    for (unsigned int i = 0; i < total_entries; i++) {
        char key_buf[32];
        snprintf(key_buf, sizeof(key_buf), "entry_%d", i);
        long long expiry_time = rand() % 10000;
        entries[i] = mockCreateEntry(key_buf, expiry_time);
        TEST_ASSERT(vsetAddEntry(set, mockGetExpiry, entries[i], expiry_time));
    }

    vsetIterator it;
    vsetStart(set, &it);

    int found[5] = {0};
    int total = 0;

    void *entry;
    while (vsetNext(&it, &entry)) {
        TEST_EXPECT(entry != NULL);
        mock_entry *e = (mock_entry *)entry;

        // Match the entries we inserted
        for (int i = 0; i < 5; i++) {
            if (strcmp(entryGetField(e), entryGetField(entries[i])) == 0) {
                found[i] = 1;
                break;
            }
        }
        total++;
    }

    TEST_ASSERT(total == 5);

    for (int i = 0; i < 5; i++) {
        TEST_EXPECT(found[i]);
    }

    vsetStop(&it);
    freeVolatileSet(set);
    for (int i = 0; i < 5; i++) mockFreeEntry(entries[i]);

    TEST_PRINT_INFO("Iterated all %d mixed expiry entries successfully", total);
    return 0;
}

int test_vset_add_and_remove_all(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    vset *set = createVolatileSet();
    TEST_ASSERT(set != NULL);

    const int total_entries = 130;
    mock_entry *entries[total_entries];
    long long expiry = 5000;

    for (int i = 0; i < total_entries; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        entries[i] = mockCreateEntry(key, expiry);
        TEST_ASSERT(vsetAddEntry(set, mockGetExpiry, entries[i], expiry));
    }

    for (int i = 0; i < total_entries; i++) {
        TEST_ASSERT(vsetRemoveEntry(set, mockGetExpiry, entries[i], expiry));
        mockFreeEntry(entries[i]);
    }

    TEST_ASSERT(vsetIsEmpty(set));
    freeVolatileSet(set);

    TEST_PRINT_INFO("Add/remove %d entries, set size now 0", total_entries);
    return 0;
}

/********************* Fuzzer tests ********************************/

#define NUM_ITERATIONS 100000
#define MAX_ENTRIES 10000

/* Global array to simulate a test database */
mock_entry *mock_entries[MAX_ENTRIES];
int mock_entry_count = 0;

/* --------- volatileEntryType Callbacks --------- */
sds mock_entry_get_key(const void *entry) {
    return (sds)entry;
}

long long mock_entry_get_expiry(const void *entry) {
    return mockGetExpiry(entry);
}

int mock_entry_expire(void *db, void *o, void *entry) {
    UNUSED(db);
    UNUSED(o);
    mock_entry *e = (mock_entry *)entry;
    for (int i = 0; i < mock_entry_count; i++) {
        if (mock_entries[i] == e) {
            // printf("expire entry %p with expiry %llu\n", e, mockGetExpiry(e));
            mockFreeEntry(e);
            mock_entries[i] = mock_entries[--mock_entry_count];
            return 1;
        }
    }
    return 0;
}

/* --------- Helper Functions --------- */
mock_entry *mock_entry_create(const char *keystr, long long expiry) {
    return mockCreateEntry(keystr, expiry);
}

int insert_mock_entry(vset *set) {
    if (mock_entry_count >= MAX_ENTRIES) return 0;
    char keybuf[32];
    snprintf(keybuf, sizeof(keybuf), "key_%d", rand());

    long long expiry = rand() % 10000 + 100;
    mock_entry *e = mock_entry_create(keybuf, expiry);
    // printf("adding entry %p with expiry %llu\n", e, expiry);
    TEST_ASSERT(vsetAddEntry(set, mockGetExpiry, e, expiry));
    mock_entries[mock_entry_count++] = e;
    return 0;
}

int update_mock_entry(vset *set) {
    if (mock_entry_count == 0) return 0;
    int idx = rand() % mock_entry_count;
    mock_entry *old = mock_entries[idx];
    long long old_expiry = mockGetExpiry(old);
    long long new_expiry = old_expiry + (rand() % 500);
    mock_entry *updated = mockEntryUpdate(old, new_expiry);
    mock_entries[idx] = updated;
    // printf("Update entry %p with entry %p with old expiry %llu new expiry %llu\n", old, updated, old_expiry, new_expiry);
    TEST_ASSERT(vsetUpdateEntry(set, mockGetExpiry, old, updated, old_expiry, new_expiry));
    return 0;
}

int remove_mock_entry(vset *set) {
    if (mock_entry_count == 0) return 0;
    int idx = rand() % mock_entry_count;
    mock_entry *e = mock_entries[idx];
    // printf("removing entry %p with expiry %llu\n", e, mockGetExpiry(e));
    TEST_ASSERT(vsetRemoveEntry(set, mockGetExpiry, e, mockGetExpiry(e)));
    mockFreeEntry(e);
    mock_entries[idx] = mock_entries[--mock_entry_count];

    return 0;
}

int expire_mock_entries(vset *set, mstime_t now) {
    void *entry;
    do {
        entry = vsetPopExpired(set, mockGetExpiry, now);
        if (entry) {
            // printf("pop expire entry %p with expiry %llu now: %llu\n", entry, mockGetExpiry(entry), now);
            TEST_ASSERT(mockGetExpiry(entry) <= now);
            mock_entry_expire(NULL, NULL, entry);
        }
    } while (entry);
    return 0;
}

int free_mock_entries(void) {
    for (int i = 0; i < mock_entry_count; i++) {
        mock_entry *e = mock_entries[i];
        mockFreeEntry(e);
    }
    return 0;
}

/* --------- Fuzzer Test --------- */
int test_vset_fuzzer(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);
    srand(time(NULL));

    vset *set = createVolatileSet();

    for (int i = 0; i < NUM_ITERATIONS; i++) {
        int op = rand() % 4;
        switch (op) {
        case 0:
        case 1:
            insert_mock_entry(set);
            break;
        case 2:
            update_mock_entry(set);
            break;
        case 3:
            remove_mock_entry(set);
            break;
        }

        if (i % 100 == 0) {
            mstime_t now = rand() % 10000;
            expire_mock_entries(set, now);
        }
    }
    /* now expire all the entries and check that we have no entries left */
    expire_mock_entries(set, LONG_LONG_MAX);
    TEST_ASSERT(vsetIsEmpty(set) && mock_entry_count == 0);
    freeVolatileSet(set);
    free_mock_entries(); /* Just in case */
    return 0;
}
