#include <string.h>
#include "volatile_set.h"
#include "rax.h"
#include "zmalloc.h"
#include "config.h"
#include "endianconv.h"
#include "serverassert.h"
#include "hashtable.h"
#include "vector.h"
#include "server.h"
#include <stdint.h>
#include "util.h"

/*************************************************************************************************************
 *                                sorted_vector Implementation
 *************************************************************************************************************/

#define SV_CARD_BITS 30
#define SV_ALLOC_BITS 34
#define SV_MAX_ELEMENTS ((1ULL << SV_CARD_BITS) - 1)
#define SV_HEADER_SIZE (sizeof(sorted_vector))
#define SV_ELEM_SIZE (sizeof(void *))
#define SV_ALLOC(pv) (pv ? pv->alloc : 0)
#define SV_LEN(pv) (pv ? pv->len : 0)
#define SV_USED_SIZE(pv) (SV_HEADER_SIZE + (SV_LEN(pv)) * SV_ELEM_SIZE)

/* Custom vector structure with embedded allocation and length counters */
typedef struct {
    uint64_t len : 30;   /* Number of elements */
    uint64_t alloc : 34; /* Allocated capacity */
    void *data[];        /* Flexible array member */
} sorted_vector;

sorted_vector *sv_grow_to_fit(sorted_vector *sv, size_t capacity) {
    size_t required = SV_HEADER_SIZE + (SV_LEN(sv) + capacity) * SV_ELEM_SIZE;
    if (SV_ALLOC(sv) >= required) return sv;

    if (!sv) {
        sv = zmalloc(required);
        sv->len = 0;
    } else {
        sv = zrealloc_usable(sv, required, &required);
    }
    sv->alloc = required;
    return sv;
}

sorted_vector *sv_shrink_to_fit(sorted_vector *sv) {
    if (!sv) return NULL;

    size_t used = SV_USED_SIZE(sv);
    size_t required = SV_HEADER_SIZE + SV_LEN(sv) * SV_ELEM_SIZE;

    if (used < required) {
        sv = zrealloc_usable(sv, used, &required);
        sv->alloc = sv->len;
    }
    return sv;
}

sorted_vector *sv_split(sorted_vector **sv_ptr, uint32_t split_index) {
    sorted_vector *sv = *sv_ptr;

    // Handle edge cases: null or empty
    if (!sv || sv->len <= 1) return NULL;

    // If no valid split found, return NULL (entire vector is one block)
    if (split_index == sv->len) return NULL;

    // Number of elements for the right half
    uint64_t right_len = sv->len - split_index;
    if (right_len == 0) return NULL;

    // Allocate new vector for right part
    size_t item_bytes = sizeof(void *);
    size_t total_bytes = sizeof(sorted_vector) + right_len * item_bytes;
    sorted_vector *right = zmalloc(total_bytes);
    right->alloc = right_len;
    right->len = right_len;

    // Copy the right part
    memcpy(&right->data[0], &sv->data[split_index], right_len * item_bytes);

    // Shrink original vector
    sv->len = split_index;
    *sv_ptr = sv_shrink_to_fit(sv); // Optional: shrink in-place to reduce memory

    return right;
}

sorted_vector *sv_new(uint32_t capacity) {
    sorted_vector *new_vec = NULL;
    return sv_grow_to_fit(new_vec, capacity);
}

sorted_vector *sv_insert(sorted_vector *sv, void *elem, uint32_t pos) {
    sv = sv_grow_to_fit(sv, 1);

    if (pos < sv->len) {
        memmove(&sv->data[pos + 1], &sv->data[pos], (sv->len - pos) * sizeof(void *));
    }

    sv->data[pos] = elem;
    sv->len++;
    return sv;
}

sorted_vector *sv_removeAt(sorted_vector *sv, uint32_t idx) {
    if (!sv || sv->len == 0) return sv;
    assert(idx < sv->len);
    if (sv->len == 1) {
        /* Last element being removed; delete vector */
        zfree(sv);
        return NULL;
    } else if (idx < sv->len - 1)
        memmove(&sv->data[idx], &sv->data[idx + 1], (sv->len - idx - 1) * SV_ELEM_SIZE);
    sv->len--;
    return sv_shrink_to_fit(sv);
}

sorted_vector *sv_remove(sorted_vector *sv, void *elem) {
    if (!sv || sv->len == 0) return sv;

    for (uint32_t i = 0; i < sv->len; i++) {
        if (sv->data[i] == elem) {
            return sv_removeAt(sv, i);
        }
    }
    return sv;
}

void *sv_get(sorted_vector *vec, uint32_t idx) {
    if (!vec || idx >= vec->len) return NULL;
    return vec->data[idx];
}

static inline uint32_t sv_len(sorted_vector *vec) {
    return SV_LEN(vec);
}

void sv_free(sorted_vector *sv) {
    if (sv) zfree(sv);
}

/*************************************************************************************************************
 *                                sorted_vector End
 *************************************************************************************************************/
#define VSET_BUCKET_NONE 0
#define VSET_BUCKET_SINGLE 1
#define VSET_BUCKET_VECTOR 2
#define VSET_BUCKET_HT 3

// TODO() We can probably embed the type in the data pointer thus save the unaligned access to the data and
// save some more bytes.
typedef struct __attribute__((__packed__)) {
    char type;
    union {
        void *single;
        sorted_vector *vector;
        hashtable *hashtable;
    } data;
} vsetBucket;

/****************** Helper Functions *******************************************/

/* compare 2 expiration times */
#define EXPIRE_COMPARE(exp1, exp2) (exp1 < exp2 ? -1 : exp1 == exp2 ? 0 \
                                                                    : 1)

static inline long long get_bucket_ts(long long expiry) {
    return (expiry & ~(VOLATILESET_BUCKET_INTERVAL_MIN - 1LL)) + VOLATILESET_BUCKET_INTERVAL_MIN;
}

/**
 * Performs binary search to find the index where the element should be inserted.
 * Returns the index where the element should be placed to keep the array sorted.
 *
 * sv Pointer to the sorted vector
 * elem Pointer to the element to insert
 * cmp Comparison function (like strcmp-style: <0, ==0, >0)
 * returns the insertion index (between 0 and sv->len) */
uint32_t _find_insert_position(volatile_set *set, vsetBucket *bucket, void *entry) {
    sorted_vector *sv = bucket->data.vector;
    uint32_t left = 0;
    uint32_t right = sv_len(sv);
    long long expiry = set->etypr->getExpiry(entry);
    while (left < right) {
        uint32_t mid = (left + right) / 2;
        int res = EXPIRE_COMPARE(expiry, set->etypr->getExpiry(sv->data[mid]));
        if (res <= 0)
            right = mid;
        else
            left = mid + 1;
    }

    return left; // Final position to insert the element
}

/* sv_find_strict_split - Finds the index at which to split a sorted vector such that:
 *                        all elements in the left part are strictly less than all elements in the right part.
 *
 * sv:       Pointer to the sorted_vector structure.
 * compare:  Comparator function taking two (const void *) arguments, returning:
 *              < 0 if a < b,
 *              0   if a == b,
 *              > 0 if a > b.
 *
 * Return: The index `i` such that:
 *           - All elements at indices [0, i-1] are strictly less than all elements at indices [i, len-1].
 *           - If no such split point exists (i.e., all elements are equal), returns sv->len.
 *
 * Behavior:
 *   - Assumes the vector is sorted in non-decreasing order.
 *   - Performs a linear scan from the beginning and finds the last index `i` such that compare(data[i], data[i+1]) < 0.
 *   - The returned index can be used to split the vector into two disjoint parts with no equal values across them.
 *
 * Example:
 *   Given vector: [1, 2, 2, 3, 4]
 *   The function may return index 3 (element 3) such that:
 *     - Left  part: [1, 2, 2]
 *     - Right part: [3, 4]
 *
 *   If all elements are equal, returns sv->len (no valid split). */
uint32_t _find_split_position(volatile_set *set, vsetBucket *bucket) {
    sorted_vector *sv = bucket->data.vector;

    if (!sv || sv->len < 2) return sv->len;

    uint32_t left = 1, right = sv->len - 1;
    uint32_t best_split = sv->len;

    while (left <= right) {
        uint32_t mid = (left + right) / 2;
        long long prev_bucket_ts = get_bucket_ts(set->etypr->getExpiry(sv->data[mid - 1]));
        long long curr_bucket_ts = get_bucket_ts(set->etypr->getExpiry(sv->data[mid]));

        int cmp = EXPIRE_COMPARE(prev_bucket_ts, curr_bucket_ts);

        if (cmp < 0) {
            best_split = mid;
            right = mid - 1; /* try to find an earlier valid split */
        } else {
            left = mid + 1;
        }
    }

    return best_split; /* may equal sv->len if no valid split found */
}


#define VSET_BUCKET_KEY_LEN 8

static uint64_t hash_pointer(const void *ptr) {
    uintptr_t x = (uintptr_t)ptr;
    if (sizeof(ptr) == 4) {
        // 32-bit platform
        x ^= x >> 16;
        x *= 0x85ebca6b;
        x ^= x >> 13;
        x *= 0xc2b2ae35;
        x ^= x >> 16;
    } else {
        // 64-bit platform
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
    }
    return (uint64_t)x;
}

hashtableType pointerHashtableType = {
    .hashFunction = hash_pointer,
};

static size_t encodeNewExpiryBucketKey(unsigned char *key, long long expiry) {
    long long bucket_ts = (expiry & ~(VOLATILESET_BUCKET_INTERVAL_MAX - 1LL)) + VOLATILESET_BUCKET_INTERVAL_MAX;
    long long be_ts = htonu64(bucket_ts);
    size_t size = sizeof(be_ts);
    memcpy(key, &be_ts, size);
    return size;
}

static inline size_t encodeExpiryKey(long long expiry, unsigned char *key) {
    long long be_ts = htonu64(expiry);
    size_t size = sizeof(be_ts);
    memcpy(key, &be_ts, size);
    return size;
}

static inline long long decodeExpiryKey(unsigned char *key) {
    long long res;
    memcpy(&res, key, sizeof(res));
    res = ntohu64(res);
    return res;
}


static inline vsetBucket *findBucket(volatile_set *set, long long expiry, unsigned char *key, size_t *key_len, long long *pbucket_ts) {
    *key_len = encodeExpiryKey(expiry, key);
    vsetBucket *bucket = NULL;

    /* First try to locate the first bucket which is larger than the specified key */
    raxIterator iter;
    raxStart(&iter, set->expiry_buckets);
    raxSeek(&iter, ">", (unsigned char *)key, *key_len);

    if (raxNext(&iter)) {
        long long bucket_ts = decodeExpiryKey(iter.key);
        /* If this bucket span over a window to far in the future, it is not a candidate. */
        if ((bucket_ts - expiry) > VOLATILESET_BUCKET_INTERVAL_MAX)
            return NULL;
        bucket = iter.data;
        if (key) {
            assert(iter.key_len == VSET_BUCKET_KEY_LEN);
            memcpy(key, iter.key, iter.key_len);
        }
        if (pbucket_ts) *pbucket_ts = decodeExpiryKey(iter.key);
    }
    raxStop(&iter);
    return bucket;
}

static bool splitBucketIfPossible(volatile_set *set, vsetBucket *bucket, long long bucket_ts) {
    /* We can only split vector encoded buckets */
    if (bucket->type != VSET_BUCKET_VECTOR) {
        return false;
    }
    size_t key_len;
    long long target_bucket_ts = bucket_ts;
    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    vsetBucket *new_bucket = NULL;
    sorted_vector *sv = bucket->data.vector;
    long long max_bucket_ts = get_bucket_ts(set->etypr->getExpiry(sv->data[sv_len(sv) - 1]));
    long long min_bucket_ts = get_bucket_ts(set->etypr->getExpiry(sv->data[0]));

    if (max_bucket_ts < bucket_ts) {
        /* In case the bucket is already spanning over a larger window than needed, just place the bucket in a new place */
        key_len = encodeExpiryKey(bucket_ts, key);
        serverAssert(raxRemove(set->expiry_buckets, key, key_len, (void **)&new_bucket));
        serverAssert(new_bucket == bucket);
        target_bucket_ts = max_bucket_ts;

    } else if (min_bucket_ts != max_bucket_ts) {
        /* lets split the bucket. we know we can do it. */
        new_bucket = zmalloc(sizeof(vsetBucket));
        uint32_t split_index = _find_split_position(set, bucket);
        assert(split_index != sv_len(sv)); /* no way to split it ???  */

        sorted_vector *bucket_vector = bucket->data.vector;
        new_bucket->data.vector = sv_split(&bucket_vector, split_index);
        bucket->data.vector = bucket_vector;
        assert(sv_len(new_bucket->data.vector) > 0);
        assert(sv_len(bucket->data.vector) > 0);
        new_bucket->type = VSET_BUCKET_VECTOR;
    } else {
        /* We cannot split the bucket. just return false */
        return false;
    }
    key_len = encodeExpiryKey(target_bucket_ts, key);
    serverAssert(raxInsert(set->expiry_buckets, key, key_len, new_bucket, NULL));
    return true;
}

volatile_set *createVolatileSet(volatileEntryType *type) {
    volatile_set *set = zmalloc(sizeof(volatile_set));
    set->etypr = type;
    set->expiry_buckets = raxNew();
    return set;
}

static void freeVsetBucket(void *entry) {
    vsetBucket *bucket = (vsetBucket *)entry;
    switch (bucket->type) {
    case VSET_BUCKET_SINGLE:
        // No internal memory to free
        break;
    case VSET_BUCKET_VECTOR:
        sv_free(bucket->data.vector);
        bucket->data.vector = NULL;
        break;
    case VSET_BUCKET_HT:
        hashtableRelease(bucket->data.hashtable);
        break;
    default:
        serverPanic("Unknown volatile set type in freeVsetBucket");
    }
    zfree(bucket);
}

void freeVolatileSet(volatile_set *b) {
    if (!b) return;
    raxFreeWithCallback(b->expiry_buckets, freeVsetBucket);
    zfree(b);
}

bool volatileSetIsEmpty(volatile_set *set) {
    return raxSize(set->expiry_buckets) == 0;
}

int volatileSetAddEntry(volatile_set *set, void *entry, long long expiry) {
    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    size_t key_len;
    long long bucket_ts;
    vsetBucket *bucket = findBucket(set, expiry, key, &key_len, &bucket_ts);
    if (!bucket) {
        /* No bucket: create single-entry bucket */
        size_t key_size = encodeNewExpiryBucketKey(key, expiry);
        bucket = zmalloc(sizeof(vsetBucket));
        bucket->type = VSET_BUCKET_SINGLE;
        bucket->data.single = entry;
        raxInsert(set->expiry_buckets, key, key_size, bucket, NULL);
        return 1;
    }

    if (bucket->type == VSET_BUCKET_NONE) {
        bucket->type = VSET_BUCKET_SINGLE;
        bucket->data.single = entry;
    } else if (bucket->type == VSET_BUCKET_SINGLE) {
        /* Upgrade to vector */
        sorted_vector *sv = sv_new(2);
        long long curr_expiry = set->etypr->getExpiry(bucket->data.single);
        if (curr_expiry < expiry) {
            sv = sv_insert(sv, bucket->data.single, 0);
            sv = sv_insert(sv, entry, 1);
        } else {
            sv = sv_insert(sv, entry, 0);
            sv = sv_insert(sv, bucket->data.single, 1);
        }
        bucket->type = VSET_BUCKET_VECTOR;
        bucket->data.vector = sv;
    } else if (bucket->type == VSET_BUCKET_VECTOR) {
        sorted_vector *sv = bucket->data.vector;
        if (sv_len(sv) == 127) {
            /* Try to split the bucket. If not possible switch to hashtable encoding. */
            if (!splitBucketIfPossible(set, bucket, bucket_ts)) {
                // Upgrade to hashtable
                hashtable *ht = hashtableCreate(&pointerHashtableType);
                sorted_vector *sv = bucket->data.vector;
                for (uint32_t i = 0; i < sv_len(sv); i++) {
                    hashtableAdd(ht, sv_get(sv, i));
                }
                sv_free(sv);
                bucket->type = VSET_BUCKET_HT;
                bucket->data.hashtable = ht;

                /* Add the new entry as well */
                hashtableAdd(ht, entry);
            } else {
                /* we splitted the bucket. go and find again a bucket to place the entry since there can be new options now. */
                volatileSetAddEntry(set, entry, expiry);
            }
        } else {
            uint32_t pos = _find_insert_position(set, bucket, entry);
            bucket->data.vector = sv_insert(sv, entry, pos);
        }
    } else if (bucket->type != VSET_BUCKET_HT) {
        serverPanic("Unknown bucket type in volatileSetAddEntry");
    }
    return 1;
}

int volatileSetRemoveEntry(volatile_set *set, void *entry, long long expiry) {
    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    long long bucket_ts;
    size_t key_len;
    vsetBucket *bucket = findBucket(set, expiry, key, &key_len, &bucket_ts);

    switch (bucket->type) {
    case VSET_BUCKET_SINGLE:
        if (bucket->data.single != entry) return 0;
        raxRemove(set->expiry_buckets, key, key_len, NULL);
        zfree(bucket);
        break;
    case VSET_BUCKET_VECTOR: {
        sorted_vector *sv = bucket->data.vector;
        if (sv_len(bucket->data.vector) == 2) {
            /* convert to single if needed */
            bucket->type = VSET_BUCKET_SINGLE;
            bucket->data.single = sv_get(sv, 0) == entry ? sv_get(sv, 1) : sv_get(sv, 0);
            sv_free(sv);
        } else {
            bucket->data.vector = sv_remove(sv, entry);
        }
        break;
    }
    case VSET_BUCKET_HT:
        if (!hashtableDelete(bucket->data.hashtable, entry)) return 0;
        if (hashtableSize(bucket->data.hashtable) == 1) {
            // Downgrade to SINGLE
            hashtableIterator hi;
            hashtableInitIterator(&hi, bucket->data.hashtable, 0);
            void *ptr;
            hashtableNext(&hi, &ptr);
            hashtableRelease(bucket->data.hashtable);
            bucket->type = VSET_BUCKET_SINGLE;
            bucket->data.single = ptr;
        }
        break;
    default:
        serverPanic("Unknown bucket type in volatileSetRemoveEntry");
        return 0;
    }
    return 1;
}

int volatileSetUpdateEntry(volatile_set *set, void *old_entry, void *new_entry, long long old_expiry, long long new_expiry) {
    if (old_entry == new_entry && old_expiry == new_expiry)
        return 1;

    if (old_entry && old_expiry != -1)
        volatileSetRemoveEntry(set, old_entry, old_expiry);

    if (new_entry && new_expiry != -1)
        volatileSetAddEntry(set, new_entry, new_expiry);

    return 1;
}

static void logField(robj *key, void *entry) {
    sds key2 = objectGetKey(key);
    serverLog(LL_WARNING, "key %s field %s value %s expired",
              key2, entryGetField(entry), entryGetValue(entry));
}

int volatileSetExpireEntry(volatile_set *set, volatileSetIterator *it, mstime_t now, void *serverDb, void *o) {
    bool empty_bucket = false;
    vsetBucket *bucket = it->bucket.data;
    /* check if we reached a bucket which end time is in the future */
    if (it->bucket_ts > now)
        return 0;

    switch (it->iteration_state) {
    case VSET_BUCKET_SINGLE:
        /* Single entry was removed. Delete the bucket */
        empty_bucket = true;
        break;

    case VSET_BUCKET_VECTOR:
        sv_removeAt(bucket->data.vector, it->viter);
        /* In case we removed an entry, we need to take a step back in the iterator in order to progress to the next entry.
         * However in case the entry is the first one, we will not be able to. So we set it to a value we know is never reached
         * in this implementation. */
        it->viter = (it->viter == 0) ? UINT32_MAX : it->viter - 1;
        empty_bucket = sv_len(bucket->data.vector) == 0;
        break;
    case VSET_BUCKET_HT:
        assert(hashtableDelete(bucket->data.hashtable, &it->entry));
        empty_bucket = hashtableSize(bucket->data.hashtable) == 0;
        hashtableResetIterator(&it->hiter);
        break;
    default:
        serverPanic("Unknown volatile set bucket type in volatileSetExpireEntry");
    }
    logField(o, it->entry);
    if (set->etypr->expire) {
        set->etypr->expire(serverDb, o, it->entry);
    }
    /* In case the bucket is empty, we can direct the iterator to continue and delete the bucket. */
    if (empty_bucket) {
        raxRemove(set->expiry_buckets, it->bucket.key, it->bucket.key_len, NULL);
        zfree(bucket);
        it->iteration_state = VSET_BUCKET_NONE;
    }
    return 1;
}

int volatileSetNext(volatileSetIterator *it, void **entryptr) {
    bool init_bucket_scan = false;
    if (it->iteration_state == VSET_BUCKET_NONE) {
        if (raxNext(&it->bucket)) {
            it->iteration_state = ((vsetBucket *)it->bucket.data)->type;
            init_bucket_scan = true;
            it->bucket_ts = decodeExpiryKey(it->bucket.key);
        } else {
            return 0;
        }
    }
    vsetBucket *bucket = it->bucket.data;
    switch (it->iteration_state) {
    case VSET_BUCKET_SINGLE:
        if (init_bucket_scan) {
            it->entry = bucket->data.single;
        } else {
            it->iteration_state = VSET_BUCKET_NONE;
            return volatileSetNext(it, entryptr);
        }
        break;
    case VSET_BUCKET_VECTOR:
        it->viter = (init_bucket_scan || it->viter == UINT32_MAX) ? 0 : (it->viter + 1);
        if (it->viter < sv_len(bucket->data.vector)) {
            it->entry = sv_get(bucket->data.vector, it->viter);
        } else {
            it->iteration_state = VSET_BUCKET_NONE;
            return volatileSetNext(it, entryptr);
        }
        break;
    case VSET_BUCKET_HT:
        if (init_bucket_scan)
            hashtableInitIterator(&it->hiter, bucket->data.hashtable, HASHTABLE_ITER_SAFE);
        if (!hashtableNext(&it->hiter, &it->entry)) {
            hashtableResetIterator(&it->hiter);
            it->iteration_state = VSET_BUCKET_NONE;
            return volatileSetNext(it, entryptr);
        }
        break;
    default:
        serverPanic("Unknown volatile set type in volatileSetNext");
    }
    if (entryptr) *entryptr = it->entry;
    return 1;
}

void volatileSetStart(volatile_set *set, volatileSetIterator *it) {
    raxStart(&it->bucket, set->expiry_buckets);
    raxSeek(&it->bucket, ">=", NULL, 0);
    it->iteration_state = VSET_BUCKET_NONE; /*lets start by going to the first bucket. */
}

void volatileSetReset(volatileSetIterator *it) {
    if (it->iteration_state == VSET_BUCKET_HT)
        hashtableResetIterator(&it->hiter);
    raxStop(&it->bucket);
}
