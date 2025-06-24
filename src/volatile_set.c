#include <string.h>
#include "volatile_set.h"
#include "rax.h"
#include "unit/test_help.h"
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
 *                                pointer_vector Implementation
 *************************************************************************************************************/

#define PV_CARD_BITS 30
#define PV_ALLOC_BITS 34
#define PV_MAX_ELEMENTS ((1ULL << PV_CARD_BITS) - 1)
#define PV_HEADER_SIZE (sizeof(pointer_vector))
#define PV_ELEM_SIZE (sizeof(void *))
#define PV_ALLOC(pv) (pv ? pv->alloc : 0)
#define PV_LEN(pv) (pv ? pv->len : 0)
#define PV_USED_SIZE(pv) (pv ? (PV_HEADER_SIZE + (pv_len(pv)) * PV_ELEM_SIZE) : 0)

/* Custom vector structure with embedded allocation and length counters */
typedef struct {
    uint64_t len : 30;   /* Number of elements */
    uint64_t alloc : 34; /* Allocated capacity */
    void *data[];        /* Flexible array member */
} pointer_vector;

/* Returns the number of elements currently stored in the pointer_vector.
 *
 * Arguments:
 *   vec - The pointer_vector to query.
 *
 * Return:
 *   The number of elements in the vector.
 *   Note that a NULL is a !!!valid!!! vector - returns 0 if the vector is NULL. */
static inline uint32_t pv_len(pointer_vector *vec) {
    return PV_LEN(vec);
}

/* Ensures that a pointer_vector has enough capacity to hold additional elements.
 *
 * This function guarantees that the given pointer_vector `sv` has at least enough
 * allocated space to accommodate `capacity` more elements, growing it if necessary.
 * If the vector is currently `NULL`, it will be newly allocated.
 *
 * The allocation is handled using `zmalloc` or `zrealloc_usable`, depending on whether
 * the vector is new or already initialized. The internal `alloc` field is updated to
 * reflect the actual allocated size.
 *
 * Arguments:
 *   sv       - Pointer to an existing pointer_vector or NULL.
 *   capacity - The number of additional elements the vector should be able to accommodate.
 *
 * Return:
 *   A pointer to the resized (or newly allocated) pointer_vector with sufficient capacity.
 *   Returns NULL only if the allocation fails.
 *
 * Note:
 *   The `capacity` is the number of *additional* elements beyond the current length.
 *   This function does not modify the vector's logical length (`len`), only its allocation. */
pointer_vector *pv_grow_to_fit(pointer_vector *sv, size_t capacity) {
    if (capacity == 0) return sv;
    size_t required = PV_HEADER_SIZE + (PV_LEN(sv) + capacity) * PV_ELEM_SIZE;
    if (PV_ALLOC(sv) >= required) return sv;

    if (!sv) {
        sv = zmalloc(required);
        sv->len = 0;
    } else {
        sv = zrealloc_usable(sv, required, &required);
    }
    sv->alloc = required;
    return sv;
}

/* Shrinks a pointer_vector to release unused allocated memory.
 *
 * This function checks if the current allocation (`used`) for the given
 * `pointer_vector` exceeds the memory actually required to store its elements.
 * If so, it reallocates the vector to use only the needed memory, helping reduce
 * memory overhead and improve space efficiency.
 *
 * The function uses `zrealloc_usable()` to reallocate memory in a way compatible
 * with jemalloc (or other zmalloc backends) and updates the internal allocation
 * size (`alloc`) to reflect the new length.
 *
 * Arguments:
 *  sv - A pointer to the `pointer_vector` to shrink.
 *
 * Return:
 *  A potentially reallocated `pointer_vector` with minimized memory usage,
 *         or `NULL` if the input was `NULL`.
 *
 *  This function does not change the logical contents of the vector.
 *  It only adjusts the allocated memory footprint. If no reallocation
 *  is needed, the original pointer is returned unchanged.
 *
 * Example:
 *     pointer_vector *vec = pv_new();
 *     // After some insertions and deletions
 *     vec = pv_shrink_to_fit(vec); */
pointer_vector *pv_shrink_to_fit(pointer_vector *sv) {
    if (!sv) return NULL;

    size_t used = PV_ALLOC(sv);
    size_t required = pv_len(sv) == 0 ? 0 : PV_HEADER_SIZE + pv_len(sv) * PV_ELEM_SIZE;

    if (used > required) {
        if (!required) {
            zfree(sv);
            return NULL;
        }
        sv = zrealloc_usable(sv, used, &required);
        sv->alloc = required;
    }
    return sv;
}

/**
 * pv_split - Splits a pointer_vector into two parts at a given index.
 *
 * Arguments:
 * sv_ptr:       A pointer to the pointer_vector* to split. This pointer is
 *                updated in-place to point to the left portion (elements [0..split_index-1]).
 * split_index:  The index at which to split the vector. The resulting right
 *                vector will contain elements [split_index..len-1].
 *
 * This function is used to **efficiently split a sorted vector of pointers**
 * into two separate vectors. The original vector is truncated in-place to
 * only contain the first half, and a new vector is returned containing the
 * second half. This allows for logical partitioning of data without scanning
 * or reallocating unnecessary memory.
 *
 * The vector is assumed to be densely packed and its elements are of type `void*`.
 *
 * Memory is allocated for the new right vector using `zmalloc`, and the unused
 * portion of the original vector may be freed or shrunk via `pv_shrink_to_fit`
 * to optimize memory usage.
 *
 * Return:
 *   - A new pointer_vector containing the right split [split_index..len-1].
 *   - `NULL` in the following cases:
 *       • The input vector is `NULL`.
 *       • The input vector has only 1 or fewer elements (nothing to split).
 *       • The `split_index` is equal to the vector length (all elements stay in the left part).
 *       • The `split_index` is such that the right part would have 0 elements.
 *
 * Side effects:
 *   - The original vector pointer (`*sv_ptr`) is modified to point to the
 *     resized left portion.
 *
 * Example:
 * --------
 * Suppose `sv_ptr` points to a vector of 5 elements:
 *     [A, B, C, D, E]
 *
 * Calling:
 *     pointer_vector *right = pv_split(&sv_ptr, 3);
 *
 * Results in:
 *     sv_ptr -> [A, B, C]
 *     right   -> [D, E]
 *
 * If the split_index is 5 (i.e. the end), the function returns NULL and the
 * original vector is unchanged. */
pointer_vector *pv_split(pointer_vector **sv_ptr, uint32_t split_index) {
    pointer_vector *sv = *sv_ptr;

    // Handle edge cases: null or empty
    if (!sv || sv->len <= 1) return NULL;

    // If no valid split found, return NULL (entire vector is one block)
    if (split_index == sv->len) return NULL;

    // Number of elements for the right half
    uint64_t right_len = sv->len - split_index;
    if (right_len == 0) return NULL;

    // Allocate new vector for right part
    size_t item_bytes = sizeof(void *);
    size_t total_bytes = sizeof(pointer_vector) + right_len * item_bytes;
    pointer_vector *right = zmalloc(total_bytes);
    right->alloc = right_len;
    right->len = right_len;

    // Copy the right part
    memcpy(&right->data[0], &sv->data[split_index], right_len * item_bytes);

    // Shrink original vector
    sv->len = split_index;
    *sv_ptr = pv_shrink_to_fit(sv); // Optional: shrink in-place to reduce memory

    return right;
}

/* Creates a new pointer_vector with the specified initial capacity.
 *
 * This function initializes a new pointer_vector capable of holding at least
 * `capacity` elements. Internally, it delegates allocation and setup to
 * `pv_grow_to_fit`, starting from a NULL vector.
 *
 * Arguments:
 *   capacity - The initial number of elements the vector should be able to store.
 *
 * Return:
 *   A pointer to the newly allocated pointer_vector.
 *   Note that a NULL is a !!valid!! cector which size is zero.
 *
 * Note:
 *   The logical length (`len`) of the returned vector is initialized to 0.
 */
pointer_vector *pv_new(uint32_t capacity) {
    pointer_vector *new_vec = NULL;
    return pv_grow_to_fit(new_vec, capacity);
}

/* Inserts an element at the specified position in the pointer_vector.
 *
 * Ensures enough capacity for the new element, shifts elements to make space,
 * and inserts the given element at the desired position.
 *
 * Arguments:
 *   sv   - The pointer_vector to insert into (can be NULL).
 *   elem - The pointer to be inserted.
 *   pos  - The index at which to insert the element (must be ≤ sv->len).
 *
 * Return:
 *   The updated pointer_vector with the element inserted. */
pointer_vector *pv_insert(pointer_vector *sv, void *elem, uint32_t pos) {
    sv = pv_grow_to_fit(sv, 1);

    if (pos < sv->len) {
        memmove(&sv->data[pos + 1], &sv->data[pos], (sv->len - pos) * sizeof(void *));
    }

    sv->data[pos] = elem;
    sv->len++;
    return sv;
}

/* Removes the element at the specified index from the pointer_vector.
 *
 * Shifts elements as necessary and optionally shrinks the vector if memory can be saved.
 * If this is the last element in the vector, the vector is freed and NULL is returned.
 *
 * Arguments:
 *   sv  - The pointer_vector to remove from.
 *   idx - The index of the element to remove (must be < sv->len).
 *
 * Return:
 *   The updated pointer_vector after removal.
 *   Returns NULL if the last element was removed and the vector was freed. */
pointer_vector *pv_removeAt(pointer_vector *sv, uint32_t idx) {
    if (!sv || sv->len == 0) return sv;
    assert(idx < sv->len);
    if (sv->len == 1) {
        /* Last element being removed; delete vector */
        zfree(sv);
        return NULL;
    } else if (idx < sv->len - 1)
        memmove(&sv->data[idx], &sv->data[idx + 1], (sv->len - idx - 1) * PV_ELEM_SIZE);
    sv->len--;
    return pv_shrink_to_fit(sv);
}

/* Removes the first matching element from the pointer_vector.
 *
 * Performs a linear search for the given pointer and removes the first match.
 * Updates the vector pointer in case a removal was done.
 *
 * Arguments:
 *   sv   - A pointer to the location of the pointer_vector to remove from.
 *   elem - The element pointer to match and remove.
 *
 * Return:
 *   true in case a removal was made, false otherwise */
bool pv_remove(pointer_vector **psv, void *elem) {
    pointer_vector *sv = *psv;
    if (!sv || sv->len == 0) return false;

    for (uint32_t i = 0; i < sv->len; i++) {
        if (sv->data[i] == elem) {
            *psv = pv_removeAt(sv, i);
            return true;
        }
    }
    return false;
}

/* Retrieves the element at the specified index in the pointer_vector.
 *
 * Arguments:
 *   vec - The pointer_vector to retrieve from.
 *   idx - The index of the element to access.
 *
 * Return:
 *   A pointer to the element at the given index.
 *   Returns NULL if the vector is NULL or the index is out of bounds. */
void *pv_get(pointer_vector *vec, uint32_t idx) {
    if (!vec || idx >= vec->len) return NULL;
    return vec->data[idx];
}

/* Frees the memory used by the pointer_vector.
 *
 * Arguments:
 *   sv - The pointer_vector to free.
 *
 * Return:
 *   None. */
void pv_free(pointer_vector *sv) {
    if (sv) zfree(sv);
}

/*************************************************************************************************************
 *                                pointer_vector End
 *************************************************************************************************************/
#define VSET_BUCKET_NONE -1  // matching the NULL case
#define VSET_BUCKET_SINGLE 1 // x1 (assuming sds)
#define VSET_BUCKET_VECTOR 0 // 00 (non-null pointer)
#define VSET_BUCKET_HT 2     // 10

// TODO() We can probably embed the type in the data pointer thus save the unaligned access to the data and
// save some more bytes.

#define VSET_TAG_MASK 0x3ULL
#define VSET_PTR_MASK (~VSET_TAG_MASK)

// Generic bucket type
typedef void vsetBucket;

// Determine bucket type
static inline int vsetBucketType(vsetBucket *b) {
    if (b == NULL) return VSET_BUCKET_NONE;

    uintptr_t bits = (uintptr_t)b;
    if (bits & 0x1)
        return VSET_BUCKET_SINGLE;
    if (bits & 0x2)
        return VSET_BUCKET_HT;
    return VSET_BUCKET_VECTOR;
}

// Access raw pointer
static inline void *vsetBucketRawPtr(vsetBucket *b) {
    return (void *)((uintptr_t)b & VSET_PTR_MASK);
}

// Accessors with type assertions
static inline pointer_vector *vsetBucketVector(vsetBucket *b) {
    assert(vsetBucketType(b) == VSET_BUCKET_VECTOR);
    return (pointer_vector *)vsetBucketRawPtr(b);
}

static inline hashtable *vsetBucketHashtable(vsetBucket *b) {
    assert(vsetBucketType(b) == VSET_BUCKET_HT);
    return (hashtable *)vsetBucketRawPtr(b);
}

static inline void *vsetBucketSingle(vsetBucket *b) {
    return b;
}

// Setters
static inline vsetBucket *vsetBucketSetType(vsetBucket *b, int type) {
    uintptr_t p = (uintptr_t)b;
    assert((p & VSET_TAG_MASK) == 0); // Must be aligned
    return (vsetBucket *)(p | (type & VSET_TAG_MASK));
}

static inline vsetBucket *vsetBucketSetVector(vsetBucket *b, pointer_vector *vec) {
    UNUSED(b);
    return vsetBucketSetType(vec, VSET_BUCKET_VECTOR);
}

static inline vsetBucket *vsetBucketSetHashtable(vsetBucket *b, hashtable *ht) {
    UNUSED(b);
    return vsetBucketSetType(ht, VSET_BUCKET_HT);
}

static inline vsetBucket *vsetBucketSetSingle(vsetBucket *b, void *ptr) {
    UNUSED(b);
    return ptr;
}

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
    pointer_vector *sv = vsetBucketVector(bucket);
    uint32_t left = 0;
    uint32_t right = pv_len(sv);
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

/* _find_split_position - Find the optimal split index in a sorted pointer vector
 *  based on coarse (bucketed) expiry timestamps.
 * Arguments
 * set:    Pointer to the `volatile_set` containing the element type and expiry logic.
 * bucket: Pointer to a `vsetBucket` holding a sorted `pointer_vector` of elements.
 * split_ts: an optional pointer to a location to store the split timestamp, that is the position
 * belonging in the lower split vector with the largest expiration time.
 *
 * This function searches for the earliest index at which the vector can be split into
 * two parts such that all elements in the first part are strictly less than all elements
 * in the second part, after mapping each element's expiry to a lower-resolution bucket.
 * The mapping is done using `get_bucket_ts(set->etypr->getExpiry(element))`.
 *
 * This ensures that elements belonging to the same coarse-grained time bucket remain
 * in the same split group, which is useful for efficient time-based partitioning.
 *
 * To do this efficiently, the function performs a binary search to locate the first
 * position where the bucketed expiry of the current item is greater than the bucketed
 * expiry of the previous item. This approach attempts to maximize the size of each
 * resulting split vector while ensuring that:
 *
 *     bucket_ts[element[i-1]] < bucket_ts[element[i]]
 *
 * If no valid split is found (i.e. all elements map to the same bucket timestamp),
 * the function returns `sv->len` to indicate that splitting is not possible.
 *
 * Return:
 *   - A valid split index in the range [1, sv->len], where the split occurs.
 *   - May return `sv->len` if no valid position is found.
 *
 * Example:
 * --------
 * Suppose the vector contains elements with matching expiry timestamps:
 *     [1234, 1235, 1236, 4567, 4568]
 *
 * And `get_bucket_ts()` maps them to:
 *     [1300, 1300, 1300, 5000, 5000]
 *
 * Then `_find_split_position(set, bucket)` returns 3, resulting in:
 *     First part:  [1234, 1235, 1236] (bucket 1300)
 *     Second part: [4567, 4568]       (bucket 5000)
 *
 * This guarantees that each vector contains elements with the same bucket timestamp,
 * and no value in the first part maps to the same or later bucket as the second part.
 */
uint32_t _find_split_position(volatile_set *set, vsetBucket *bucket, long long *split_ts) {
    pointer_vector *sv = vsetBucketVector(bucket);

    if (!sv || sv->len < 2) return sv->len;

    uint32_t left = 1, right = sv->len - 1;
    uint32_t best_split = sv->len;

    while (left <= right) {
        uint32_t mid = (left + right) / 2;
        long long prev_bucket_ts = get_bucket_ts(set->etypr->getExpiry(pv_get(sv, mid - 1)));
        long long curr_bucket_ts = get_bucket_ts(set->etypr->getExpiry(pv_get(sv, mid)));

        int cmp = EXPIRE_COMPARE(prev_bucket_ts, curr_bucket_ts);

        if (cmp < 0) {
            if (split_ts) *split_ts = prev_bucket_ts;
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


static inline vsetBucket *findBucket(volatile_set *set, long long expiry, unsigned char *key, size_t *key_len, long long *pbucket_ts, raxNode **node) {
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
        assert(iter.node->iskey);
        if (node) *node = iter.node;
        if (key) {
            assert(iter.key_len == VSET_BUCKET_KEY_LEN);
            memcpy(key, iter.key, iter.key_len);
        }
        if (pbucket_ts) *pbucket_ts = decodeExpiryKey(iter.key);
    }
    raxStop(&iter);
    return bucket;
}

static bool splitBucketIfPossible(volatile_set *set, vsetBucket *bucket, long long bucket_ts, raxNode *node) {
    /* We can only split vector encoded buckets */
    if (vsetBucketType(bucket) != VSET_BUCKET_VECTOR) {
        return false;
    }
    size_t key_len;
    long long target_bucket_ts = bucket_ts;
    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    vsetBucket *new_bucket = NULL;
    pointer_vector *sv = vsetBucketVector(bucket);
    long long max_bucket_ts = get_bucket_ts(set->etypr->getExpiry(sv->data[pv_len(sv) - 1]));
    long long min_bucket_ts = get_bucket_ts(set->etypr->getExpiry(sv->data[0]));

    if (max_bucket_ts < bucket_ts) {
        /* In case the bucket is already spanning over a larger window than needed, just place the bucket in a new place */
        key_len = encodeExpiryKey(bucket_ts, key);
        serverAssert(raxRemove(set->expiry_buckets, key, key_len, (void **)&new_bucket));
        serverAssert(new_bucket == bucket);
        target_bucket_ts = max_bucket_ts;

    } else if (min_bucket_ts != max_bucket_ts) {
        /* lets split the bucket. we know we can do it. */
        uint32_t split_index = _find_split_position(set, bucket, &target_bucket_ts);
        assert(split_index != pv_len(sv)); /* no way to split it ???  */
        pointer_vector *new_bucket_vector = vsetBucketVector(bucket);
        bucket = vsetBucketSetVector(bucket, pv_split(&new_bucket_vector, split_index));
        new_bucket = vsetBucketSetVector(new_bucket, new_bucket_vector);
        assert(pv_len(vsetBucketVector(new_bucket)) > 0);
        assert(pv_len(vsetBucketVector(bucket)) > 0);
        /* modify the current bucket data pointer */
        key_len = encodeExpiryKey(bucket_ts, key);
        /* In order to avoid rax override, we directly change the node data */
        // alternative: raxInsert(set->expiry_buckets, key, key_len, bucket, NULL);
        raxSetData(node, bucket);
    } else {
        /* We cannot split the bucket. just return false */
        return false;
    }
    /* We change the current bucket position OR we splited it, either way we have a new bucket to insert. */
    key_len = encodeExpiryKey(target_bucket_ts, key);
    raxInsert(set->expiry_buckets, key, key_len, new_bucket, NULL);
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
    switch (vsetBucketType(bucket)) {
    case VSET_BUCKET_SINGLE:
        // No internal memory to free
        break;
    case VSET_BUCKET_VECTOR:
        pv_free(vsetBucketVector(bucket));
        break;
    case VSET_BUCKET_HT:
        hashtableRelease(vsetBucketHashtable(bucket));
        break;
    default:
        serverPanic("Unknown volatile set type in freeVsetBucket");
    }
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
    raxNode *node;
    vsetBucket *bucket = findBucket(set, expiry, key, &key_len, &bucket_ts, &node);
    if (!bucket) {
        /* No bucket: create single-entry bucket */
        size_t key_size = encodeNewExpiryBucketKey(key, expiry);
        bucket = vsetBucketSetSingle(bucket, entry);
        raxInsert(set->expiry_buckets, key, key_size, bucket, NULL);
        return 1;
    } else if (vsetBucketType(bucket) == VSET_BUCKET_SINGLE) {
        /* Upgrade to vector */
        pointer_vector *sv = pv_new(2);
        void *curr_entry = vsetBucketSingle(bucket);
        long long curr_expiry = set->etypr->getExpiry(curr_entry);
        if (curr_expiry < expiry) {
            sv = pv_insert(sv, curr_entry, 0);
            sv = pv_insert(sv, entry, 1);
        } else {
            sv = pv_insert(sv, entry, 0);
            sv = pv_insert(sv, curr_entry, 1);
        }
        bucket = vsetBucketSetVector(bucket, sv);
        /* In order to avoid rax override, we directly change the node data */
        // alternative: raxInsert(set->expiry_buckets, key, key_len, bucket, NULL);
        raxSetData(node, bucket);
    } else if (vsetBucketType(bucket) == VSET_BUCKET_VECTOR) {
        pointer_vector *sv = vsetBucketVector(bucket);
        if (pv_len(sv) == 127) {
            /* Try to split the bucket. If not possible switch to hashtable encoding. */
            if (!splitBucketIfPossible(set, bucket, bucket_ts, node)) {
                // Upgrade to hashtable
                hashtable *ht = hashtableCreate(&pointerHashtableType);
                pointer_vector *sv = vsetBucketVector(bucket);
                for (uint32_t i = 0; i < pv_len(sv); i++) {
                    hashtableAdd(ht, pv_get(sv, i));
                }
                pv_free(sv);
                /* Add the new entry as well */
                hashtableAdd(ht, entry);

                bucket = vsetBucketSetHashtable(bucket, ht);
                /* In order to avoid rax override, we directly change the node data */
                // alternative raxInsert(set->expiry_buckets, key, key_len, bucket, NULL);
                raxSetData(node, bucket);
            } else {
                /* we splitted the bucket. go and find again a bucket to place the entry since there can be new options now. */
                volatileSetAddEntry(set, entry, expiry);
            }
        } else {
            uint32_t pos = _find_insert_position(set, bucket, entry);
            vsetBucket *new_bucket = vsetBucketSetVector(bucket, pv_insert(sv, entry, pos));
            if (new_bucket != bucket)
                /* In order to avoid rax override, we directly change the node data */
                // alternative: raxInsert(set->expiry_buckets, key, key_len, new_bucket, NULL);
                raxSetData(node, new_bucket);
        }
    } else if (vsetBucketType(bucket) == VSET_BUCKET_HT) {
        hashtable *ht = vsetBucketHashtable(bucket);
        assert(hashtableAdd(ht, entry));
    } else {
        serverPanic("Unknown bucket type in volatileSetAddEntry");
    }
    return 1;
}

static int volatileSetRemoveEntryFromBucket(volatile_set *set, void *entry, vsetBucket *bucket, unsigned char *key, size_t key_len, vsetBucket **pbucket, raxNode *node) {
    switch (vsetBucketType(bucket)) {
    case VSET_BUCKET_SINGLE:
        if (vsetBucketSingle(bucket) != entry) return 0;
        raxRemove(set->expiry_buckets, key, key_len, NULL);
        if (pbucket) *pbucket = NULL;
        break;
    case VSET_BUCKET_VECTOR: {
        vsetBucket *new_bucket;
        pointer_vector *sv = vsetBucketVector(bucket);
        /* In case we we removed the entry */
        if (pv_remove(&sv, entry)) {
            if (pv_len(sv) == 1) {
                /* convert to single if needed */
                new_bucket = vsetBucketSetSingle(bucket, pv_get(sv, 0));
                pv_free(sv);
            } else if (pv_len(sv) == 0) {
                raxRemove(set->expiry_buckets, key, key_len, NULL);
                if (pbucket) *pbucket = NULL;
                break;
            } else {
                new_bucket = vsetBucketSetVector(bucket, sv);
            }
            // if (new_bucket != bucket)
            /* In order to avoid rax override, we directly change the node data */
            // alternative: raxInsert(set->expiry_buckets, key, key_len, new_bucket, NULL);
            raxSetData(node, new_bucket);
            if (pbucket) *pbucket = new_bucket;
        } else
            return 0;
        break;
    }
    case VSET_BUCKET_HT: {
        hashtable *ht = vsetBucketHashtable(bucket);
        if (!hashtableDelete(ht, entry)) return 0;
        assert(hashtableSize(ht) > 0);
        if (hashtableSize(ht) == 1) {
            // Downgrade to SINGLE
            hashtableIterator hi;
            hashtableInitIterator(&hi, ht, 0);
            void *ptr;
            hashtableNext(&hi, &ptr);
            hashtableRelease(ht);
            bucket = vsetBucketSetSingle(bucket, ptr);
            /* In order to avoid rax override, we directly change the node data */
            // alternative: raxInsert(set->expiry_buckets, key, key_len, bucket, NULL);
            raxSetData(node, bucket);
        }
        if (pbucket) *pbucket = bucket;
        break;
    }
    default:
        serverPanic("Unknown bucket type in volatileSetRemoveEntry");
        return 0;
    }
    return 1;
}
int volatileSetRemoveEntry(volatile_set *set, void *entry, long long expiry) {
    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    long long bucket_ts;
    size_t key_len;
    raxNode *node;
    vsetBucket *bucket = findBucket(set, expiry, key, &key_len, &bucket_ts, &node);
    assert(bucket);
    return volatileSetRemoveEntryFromBucket(set, entry, bucket, key, key_len, NULL, node);
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

int volatileSetExpireEntry(volatile_set *set, volatileSetIterator *it, mstime_t now, void *serverDb, void *o) {
    vsetBucket *bucket = it->bucket.data;
    raxNode *node = it->bucket.node;
    /* check if we reached a bucket which end time is in the future */
    if (it->bucket_ts > now)
        return 0;

    assert(volatileSetRemoveEntryFromBucket(set, it->entry, bucket, it->bucket.key, it->bucket.key_len, &bucket, node));

    /* call expire on the entry */
    if (set->etypr->expire) {
        set->etypr->expire(serverDb, o, it->entry);
    }

    /* Must reposition the iterator after deletion */
    if (!bucket) {
        raxSeek(&it->bucket, "^", NULL, 0);
        it->iteration_state = VSET_BUCKET_NONE;
    } else {
        it->bucket.data = bucket;
        it->iteration_state = vsetBucketType(bucket);
    }
    return 1;
}

int volatileSetNext(volatileSetIterator *it, void **entryptr) {
    bool init_bucket_scan = false;
    if (it->iteration_state == VSET_BUCKET_NONE) {
        if (raxNext(&it->bucket)) {
            it->iteration_state = vsetBucketType(it->bucket.data);
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
            it->entry = vsetBucketSingle(bucket);
        } else {
            it->iteration_state = VSET_BUCKET_NONE;
            return volatileSetNext(it, entryptr);
        }
        break;
    case VSET_BUCKET_VECTOR: {
        pointer_vector *pv = vsetBucketVector(bucket);
        it->viter = (init_bucket_scan || it->viter == UINT32_MAX) ? 0 : (it->viter + 1);
        if (it->viter < pv_len(pv)) {
            it->entry = pv_get(pv, it->viter);
        } else {
            it->iteration_state = VSET_BUCKET_NONE;
            return volatileSetNext(it, entryptr);
        }
        break;
    }
    case VSET_BUCKET_HT: {
        hashtable *ht = vsetBucketHashtable(bucket);
        if (init_bucket_scan)
            hashtableInitIterator(&it->hiter, ht, HASHTABLE_ITER_SAFE);
        if (!hashtableNext(&it->hiter, &it->entry)) {
            hashtableResetIterator(&it->hiter);
            it->iteration_state = VSET_BUCKET_NONE;
            return volatileSetNext(it, entryptr);
        }
        break;
    }
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
