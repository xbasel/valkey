#include "vset.h"
#include "rax.h"
#include "endianconv.h"
#include "serverassert.h"
#include "hashtable.h"
#include "util.h"
#include "zmalloc.h"

#include <string.h>
#include <stdint.h>
#include <stdlib.h>

/*************************************************************************************************************
 *                                pVector Implementation
 *************************************************************************************************************/

#define PV_CARD_BITS 30
#define PV_ALLOC_BITS 34
#define PV_MAX_ELEMENTS ((1ULL << PV_CARD_BITS) - 1)
#define PV_HEADER_SIZE (sizeof(pVector))
#define PV_ELEM_SIZE (sizeof(void *))
#define PV_ALLOC(pv) (pv ? pv->alloc : 0)
#define PV_LEN(pv) (pv ? pv->len : 0)
#define PV_USED_SIZE(pv) (pv ? (PV_HEADER_SIZE + (pvLen(pv)) * PV_ELEM_SIZE) : 0)

/* Custom vector structure with embedded allocation and length counters */
typedef struct {
    uint64_t len : 30;   /* Number of elements (cardinality) */
    uint64_t alloc : 34; /* Allocated memory (zmalloc_size of the current vector allocation) */
    void *data[];        /* Flexible array member */
} pVector;

/* Returns the number of elements currently stored in the pVector.
 *
 * Arguments:
 *   vec - The pVector to query.
 *
 * Return:
 *   The number of elements in the vector.
 *   Note that a NULL is a !!!valid!!! vector - returns 0 if the vector is NULL. */
static inline uint32_t pvLen(pVector *vec) {
    return PV_LEN(vec);
}

/* Ensures that a pVector has enough capacity to hold additional elements.
 *
 * This function guarantees that the given pVector `sv` has at least enough
 * allocated space to accommodate `additional` more elements, growing it if necessary.
 * If the vector is currently `NULL`, it will be newly allocated.
 *
 * The allocation is handled using `zmalloc` or `zrealloc_usable`, depending on whether
 * the vector is new or already initialized. The internal `alloc` field is updated to
 * reflect the actual allocated size.
 *
 * Arguments:
 *   sv       - Pointer to an existing pVector or NULL.
 *   additional - The number of additional elements the vector should be able to accommodate.
 *
 * Return:
 *   A pointer to the resized (or newly allocated) pVector with sufficient capacity.
 *   Returns NULL only if the allocation fails.
 *
 * Note:
 *   The `additional` is the number of *additional* elements beyond the current length.
 *   This function does not modify the vector's logical length (`len`), only its allocation. */
pVector *pvMakeRoomFor(pVector *sv, size_t additional) {
    if (additional == 0) return sv;
    size_t required = PV_HEADER_SIZE + (PV_LEN(sv) + additional) * PV_ELEM_SIZE;
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

/* Shrinks a pVector to release unused allocated memory.
 *
 * This function checks if the current allocation (`used`) for the given
 * `pVector` exceeds the memory actually required to store its elements.
 * If so, it reallocates the vector to use only the needed memory, helping reduce
 * memory overhead and improve space efficiency.
 *
 * The function uses `zrealloc_usable()` to reallocate memory in a way compatible
 * with jemalloc (or other zmalloc backends) and updates the internal allocation
 * size (`alloc`) to reflect the new length.
 *
 * Arguments:
 *  sv - A pointer to the `pVector` to shrink.
 *
 * Return:
 *  A potentially reallocated `pVector` with minimized memory usage,
 *         or `NULL` if the input was `NULL`.
 *
 *  This function does not change the logical contents of the vector.
 *  It only adjusts the allocated memory footprint. If no reallocation
 *  is needed, the original pointer is returned unchanged.
 *
 * Example:
 *     pVector *vec = pvNew();
 *     // After some insertions and deletions
 *     vec = pvShrinkToFit(vec); */
pVector *pvShrinkToFit(pVector *sv) {
    if (!sv) return NULL;

    size_t used = PV_ALLOC(sv);
    size_t required = pvLen(sv) == 0 ? 0 : PV_HEADER_SIZE + pvLen(sv) * PV_ELEM_SIZE;

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
 * pvSplit - Splits a pVector into two parts at a given index.
 *
 * Arguments:
 * sv_ptr:       A pointer to the pVector* to split. This pointer is
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
 * portion of the original vector may be freed or shrunk via `pvShrinkToFit`
 * to optimize memory usage.
 *
 * Return:
 *   - A new pVector containing the right split [split_index..len-1].
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
 *     pVector *right = pvSplit(&sv_ptr, 3);
 *
 * Results in:
 *     sv_ptr -> [A, B, C]
 *     right   -> [D, E]
 *
 * If the split_index is 5 (i.e. the end), the function returns NULL and the
 * original vector is unchanged. */
pVector *pvSplit(pVector **sv_ptr, uint32_t split_index) {
    pVector *sv = *sv_ptr;

    // Handle edge cases: null or empty
    if (!sv || sv->len <= 1) return NULL;

    // If no valid split found, return NULL (entire vector is one block)
    if (split_index == sv->len) return NULL;

    // Number of elements for the right half
    uint64_t right_len = sv->len - split_index;
    if (right_len == 0) return NULL;

    // Allocate new vector for right part
    size_t item_bytes = sizeof(void *);
    size_t total_bytes = sizeof(pVector) + right_len * item_bytes;
    size_t new_alloc;
    pVector *right = zmalloc_usable(total_bytes, &new_alloc);
    right->alloc = new_alloc;
    right->len = right_len;

    // Copy the right part
    memcpy(&right->data[0], &sv->data[split_index], right_len * item_bytes);

    // Shrink original vector
    sv->len = split_index;
    *sv_ptr = pvShrinkToFit(sv); // Optional: shrink in-place to reduce memory

    return right;
}

/* Creates a new pVector with the specified initial capacity.
 *
 * This function initializes a new pVector capable of holding at least
 * `capacity` elements. Internally, it delegates allocation and setup to
 * `pvMakeRoomFor`, starting from a NULL vector.
 *
 * Arguments:
 *   capacity - The initial number of elements the vector should be able to store.
 *
 * Return:
 *   A pointer to the newly allocated pVector.
 *   Note that a NULL is a !!valid!! cector which size is zero.
 *
 * Note:
 *   The logical length (`len`) of the returned vector is initialized to 0.
 */
pVector *pvNew(uint32_t capacity) {
    pVector *new_vec = NULL;
    return pvMakeRoomFor(new_vec, capacity);
}

/* Inserts an element at the specified position in the pVector.
 *
 * Ensures enough capacity for the new element, shifts elements to make space,
 * and inserts the given element at the desired position.
 *
 * Arguments:
 *   sv   - The pVector to insert into (can be NULL).
 *   elem - The pointer to be inserted.
 *   pos  - The index at which to insert the element (must be ≤ sv->len).
 *
 * Return:
 *   The updated pVector with the element inserted. */
pVector *pvInsert(pVector *sv, void *elem, uint32_t pos) {
    sv = pvMakeRoomFor(sv, 1);

    if (pos < sv->len) {
        memmove(&sv->data[pos + 1], &sv->data[pos], (sv->len - pos) * sizeof(void *));
    }

    sv->data[pos] = elem;
    sv->len++;
    return sv;
}

/* Removes the element at the specified index from the pVector.
 *
 * Shifts elements as necessary and optionally shrinks the vector if memory can be saved.
 * If this is the last element in the vector, the vector is freed and NULL is returned.
 *
 * Arguments:
 *   sv  - The pVector to remove from.
 *   idx - The index of the element to remove (must be < sv->len).
 *
 * Return:
 *   The updated pVector after removal.
 *   Returns NULL if the last element was removed and the vector was freed. */
pVector *pvRemoveAt(pVector *sv, uint32_t idx) {
    if (!sv || sv->len == 0) return sv;
    assert(idx < sv->len);
    if (sv->len == 1) {
        /* Last element being removed; delete vector */
        zfree(sv);
        return NULL;
    } else if (idx < sv->len - 1UL)
        memmove(&sv->data[idx], &sv->data[idx + 1], (sv->len - idx - 1) * PV_ELEM_SIZE);
    sv->len--;
    return pvShrinkToFit(sv);
}

/* Removes the first matching element from the pVector.
 *
 * Performs a linear search for the given pointer and removes the first match.
 * Updates the vector pointer in case a removal was done.
 *
 * Arguments:
 *   sv   - A pointer to the location of the pVector to remove from.
 *   elem - The element pointer to match and remove.
 *
 * Return:
 *   true in case a removal was made, false otherwise */
bool pvRemove(pVector **psv, void *elem) {
    pVector *sv = *psv;
    if (!sv || sv->len == 0) return false;

    for (uint32_t i = 0; i < sv->len; i++) {
        if (sv->data[i] == elem) {
            *psv = pvRemoveAt(sv, i);
            return true;
        }
    }
    return false;
}

/* Retrieves the element at the specified index in the pVector.
 *
 * Arguments:
 *   vec - The pVector to retrieve from.
 *   idx - The index of the element to access.
 *
 * Return:
 *   A pointer to the element at the given index.
 *   Returns NULL if the vector is NULL or the index is out of bounds. */
void *pvGet(pVector *vec, uint32_t idx) {
    if (!vec || idx >= vec->len) return NULL;
    return vec->data[idx];
}

/* Frees the memory used by the pVector.
 *
 * Arguments:
 *   sv - The pVector to free.
 *
 * Return:
 *   None. */
void pvFree(pVector *sv) {
    if (sv) zfree(sv);
}

uint32_t pvFind(pVector *sv, void *elem) {
    if (!sv || sv->len == 0) return 0;

    for (uint32_t i = 0; i < sv->len; i++) {
        if (sv->data[i] == elem) {
            return i;
        }
    }
    return sv->len;
}
/*************************************************************************************************************
 *                                pVector End
 *************************************************************************************************************/
#define VSET_BUCKET_NONE -1      // matching the NULL case
#define VSET_BUCKET_SINGLE 0x1UL // xx1 (assuming sds)
#define VSET_BUCKET_VECTOR 0x2UL // 010
#define VSET_BUCKET_HT 0x4UL     // 100
#define VSET_BUCKET_RAX 0x6UL    // 110

#define VSET_TAG_MASK 0x7UL
#define VSET_PTR_MASK (~VSET_TAG_MASK)

// Determine bucket type
static inline int vsetBucketType(vsetBucket *b) {
    if (b == NULL) return VSET_BUCKET_NONE;

    uintptr_t bits = (uintptr_t)b;
    if (bits & 0x1)
        return VSET_BUCKET_SINGLE;
    return bits & VSET_TAG_MASK;
}

// Access raw pointer
static inline void *vsetBucketRawPtr(vsetBucket *b) {
    return (void *)((uintptr_t)b & VSET_PTR_MASK);
}

// Accessors with type assertions
static inline pVector *vsetBucketVector(vsetBucket *b) {
    assert(vsetBucketType(b) == VSET_BUCKET_VECTOR);
    return (pVector *)vsetBucketRawPtr(b);
}

static inline hashtable *vsetBucketHashtable(vsetBucket *b) {
    assert(vsetBucketType(b) == VSET_BUCKET_HT);
    return (hashtable *)vsetBucketRawPtr(b);
}

static inline rax *vsetBucketRax(vsetBucket *b) {
    assert(vsetBucketType(b) == VSET_BUCKET_RAX);
    return (rax *)vsetBucketRawPtr(b);
}

static inline void *vsetBucketSingle(vsetBucket *b) {
    return b;
}

// Setters
static inline vsetBucket *vsetBucketFromRawPtr(void *ptr, int type) {
    uintptr_t p = (uintptr_t)ptr;
    return (vsetBucket *)(p | (type & VSET_TAG_MASK));
}

static inline vsetBucket *vsetBucketFromVector(pVector *vec) {
    return vsetBucketFromRawPtr(vec, VSET_BUCKET_VECTOR);
}

static inline vsetBucket *vsetBucketFromHashtable(hashtable *ht) {
    return vsetBucketFromRawPtr(ht, VSET_BUCKET_HT);
}

static inline vsetBucket *vsetBucketFromSingle(void *ptr) {
    return ptr;
}

static inline vsetBucket *vsetBucketFromNone(void) {
    return NULL;
}

static inline vsetBucket *vsetBucketFromRax(rax *r) {
    return vsetBucketFromRawPtr(r, VSET_BUCKET_RAX);
}

/****************** Helper Functions *******************************************/

/* compare 2 expiration times */
#define EXPIRE_COMPARE(exp1, exp2) (exp1 < exp2 ? -1 : exp1 == exp2 ? 0 \
                                                                    : 1)

static inline long long get_bucket_ts(long long expiry) {
    return (expiry & ~(VOLATILESET_BUCKET_INTERVAL_MIN - 1LL)) + VOLATILESET_BUCKET_INTERVAL_MIN;
}

static inline long long get_max_bucket_ts(long long expiry) {
    return (expiry & ~(VOLATILESET_BUCKET_INTERVAL_MAX - 1LL)) + VOLATILESET_BUCKET_INTERVAL_MAX;
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

static inline size_t encodeNewExpiryBucketKey(unsigned char *key, long long expiry) {
    long long bucket_ts = get_max_bucket_ts(expiry);
    long long be_ts = htonu64(bucket_ts);
    size_t size = sizeof(be_ts);
    memcpy(key, &be_ts, size);
    return size;
}

/**
 * Performs binary search to find the index where the element should be inserted.
 * Returns the index where the element should be placed to keep the array sorted.
 *
 * sv Pointer to the sorted vector
 * elem Pointer to the element to insert
 * cmp Comparison function (like strcmp-style: <0, ==0, >0)
 * returns the insertion index (between 0 and sv->len) */
static inline uint32_t findInsertPosition(vsetGetExpiryFunc getExpiry, vsetBucket *bucket, long long expiry) {
    pVector *pv = vsetBucketVector(bucket);
    uint32_t left = 0;
    uint32_t right = pvLen(pv);
    while (left < right) {
        uint32_t mid = (left + right) / 2;
        int res = EXPIRE_COMPARE(expiry, getExpiry(pv->data[mid]));
        if (res <= 0)
            right = mid;
        else
            left = mid + 1;
    }

    return left; // Final position to insert the element
}

/* findSplitPosition - Find the optimal split index in a sorted pointer vector
 *  based on coarse (bucketed) expiry timestamps.
 * Arguments
 * set:    Pointer to the `vset` containing the element type and expiry logic.
 * bucket: Pointer to a `vsetBucket` holding a sorted `pVector` of elements.
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
 * Then `findSplitPosition(set, bucket)` returns 3, resulting in:
 *     First part:  [1234, 1235, 1236] (bucket 1300)
 *     Second part: [4567, 4568]       (bucket 5000)
 *
 * This guarantees that each vector contains elements with the same bucket timestamp,
 * and no value in the first part maps to the same or later bucket as the second part.
 */
static uint32_t findSplitPosition(vsetGetExpiryFunc getExpiry, vsetBucket *bucket, long long *split_ts_out) {
    pVector *pv = vsetBucketVector(bucket);

    if (!pv || pv->len < 2) return pv ? pv->len : 0;

    uint32_t left = 1;
    uint32_t right = pv->len - 1;
    uint32_t best_split = pv->len;
    uint32_t mid_closest_to_center = pv->len / 2;
    long long best_split_ts = 0;

    while (left <= right) {
        uint32_t mid = (left + right) / 2;

        long long prev_ts = get_bucket_ts(getExpiry(pvGet(pv, mid - 1)));
        long long curr_ts = get_bucket_ts(getExpiry(pvGet(pv, mid)));

        if (prev_ts != curr_ts) {
            // Check if closer to center
            if (best_split == pv->len ||
                abs((int)mid - (int)mid_closest_to_center) < abs((int)best_split - (int)mid_closest_to_center)) {
                best_split = mid;
                best_split_ts = prev_ts;
            }
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    if (split_ts_out) {
        *split_ts_out = best_split != pv->len
                            ? best_split_ts
                            : get_bucket_ts(getExpiry(pvGet(pv, pv->len - 1)));
    }

    return best_split;
}


#define VSET_BUCKET_KEY_LEN 8

/* hash_pointer - Computes a high-quality 64-bit hash from a pointer value.
 *
 * This function is designed to produce a well-distributed hash from a memory
 * pointer, avoiding the common pitfall of poor entropy due to pointer alignment.
 * It uses a platform-dependent mixing strategy based on MurmurHash3 finalization
 * constants, ensuring good avalanche behavior and low collision rates.
 *
 * For 32-bit systems:
 *   The function uses a reduced MurmurHash3 32-bit finalizer:
 *     - XORs and right shifts to mix higher-order bits into lower ones.
 *     - Multiplies by large constants to further spread the bits.
 *
 *
 * For 64-bit systems:
 *   The function uses MurmurHash3 64-bit finalizer constants:
 *     - These constants are chosen to maximize bit diffusion and avoid hash clustering.
 *     - This version benefits from the full 64-bit pointer space.
 *
 * Why this works:
 *   - Pointers tend to have low entropy in their lower bits (due to alignment).
 *   - A naive cast to integer leads to clustering and collisions in hash tables.
 *   - This function performs fast and effective bit mixing to reduce collisions.
 *   - Ideal for use in pointer-keyed hash tables, interning systems, or caches.
 *
 * Note:
 *   - This is not a cryptographic hash. It is suitable for fast, internal use only.
 *   - Returns a 64-bit hash value, even on 32-bit systems.
 *
 * Returns:
 *   A 64-bit hash value derived from the input pointer. */
static uint64_t hash_pointer(const void *ptr) {
    uintptr_t x = (uintptr_t)ptr;
#if UINTPTR_MAX == 0xFFFFFFFF
    // 32-bit platform
    x ^= x >> 16;
    x *= 0x85ebca6b;
    x ^= x >> 13;
    x *= 0xc2b2ae35;
    x ^= x >> 16;

#else
    // 64-bit platform
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
#endif
    return (uint64_t)x;
}

hashtableType pointerHashtableType = {
    .hashFunction = hash_pointer,
};

static inline vsetBucket *findBucket(rax *expiry_buckets, long long expiry, unsigned char *key, size_t *key_len, long long *pbucket_ts, raxNode **node) {
    *key_len = encodeExpiryKey(expiry, key);
    vsetBucket *bucket = NULL;
    /* First try to locate the first bucket which is larger than the specified key */
    raxIterator iter;
    raxStart(&iter, expiry_buckets);
    raxSeek(&iter, ">", (unsigned char *)key, *key_len);

    if (raxNext(&iter)) {
        long long bucket_ts = decodeExpiryKey(iter.key);
        /* If this bucket span over a window to far in the future, it is not a candidate. */
        if (get_max_bucket_ts(expiry) < bucket_ts) {
            raxStop(&iter);
            return NULL;
        }
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

/* Free all the vsetBucket memory.
 * Since the bucket only holds references to entries the entries themselves are NOT freed */
static void freeVsetBucket(vsetBucket *bucket) {
    switch (vsetBucketType(bucket)) {
    case VSET_BUCKET_NONE:
    case VSET_BUCKET_SINGLE:
        // No internal memory to free
        break;
    case VSET_BUCKET_VECTOR:
        pvFree(vsetBucketVector(bucket));
        break;
    case VSET_BUCKET_HT:
        hashtableRelease(vsetBucketHashtable(bucket));
        break;
    case VSET_BUCKET_RAX:
        raxFreeWithCallback(vsetBucketRax(bucket), freeVsetBucket);
        break;
    default:
        panic("Unknown volatile set type in freeVsetBucket");
    }
}

static bool splitBucketIfPossible(vsetBucket *parent, vsetGetExpiryFunc getExpiry, vsetBucket *bucket, long long bucket_ts, raxNode *node) {
    /* We can only split vector encoded buckets */
    if (vsetBucketType(bucket) != VSET_BUCKET_VECTOR) {
        return false;
    }
    size_t key_len;
    long long target_bucket_ts = bucket_ts;
    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    vsetBucket *new_bucket = NULL;
    pVector *sv = vsetBucketVector(bucket);
    rax *expiry_buckets = vsetBucketRax(parent);
    long long max_bucket_ts = get_bucket_ts(getExpiry(sv->data[pvLen(sv) - 1]));
    long long min_bucket_ts = get_bucket_ts(getExpiry(sv->data[0]));

    if (max_bucket_ts < bucket_ts) {
        /* In case the bucket is already spanning over a larger window than needed, just place the bucket in a new place */
        key_len = encodeExpiryKey(bucket_ts, key);
        assert(raxRemove(expiry_buckets, key, key_len, (void **)&new_bucket));
        assert(new_bucket == bucket);
        target_bucket_ts = max_bucket_ts;

    } else if (min_bucket_ts != max_bucket_ts) {
        /* lets split the bucket. we know we can do it. */
        uint32_t split_index = findSplitPosition(getExpiry, bucket, &target_bucket_ts);
        assert(target_bucket_ts < bucket_ts);
        assert(split_index != pvLen(sv)); /* no way to split it ???  */
        pVector *new_bucket_vector = vsetBucketVector(bucket);
        bucket = vsetBucketFromVector(pvSplit(&new_bucket_vector, split_index));
        new_bucket = vsetBucketFromVector(new_bucket_vector);
        assert(pvLen(vsetBucketVector(new_bucket)) > 0);
        assert(pvLen(vsetBucketVector(bucket)) > 0);
        /* modify the current bucket data pointer */
        key_len = encodeExpiryKey(bucket_ts, key);
        /* In order to avoid rax override, we directly change the node data */
        // alternative: raxInsert(*set, key, key_len, bucket, NULL);
        raxSetData(node, bucket);

    } else {
        /* We cannot split the bucket. just return false */
        return false;
    }
    /* We change the current bucket position OR we split it, either way we have a new bucket to insert. */
    key_len = encodeExpiryKey(target_bucket_ts, key);
    raxInsert(expiry_buckets, key, key_len, new_bucket, NULL);
    return true;
}

static inline vsetBucket *insertToBucket_NONE(vsetGetExpiryFunc getExpiry, vsetBucket *bucket, void *entry, long long expiry) {
    UNUSED(getExpiry);
    UNUSED(expiry);
    UNUSED(bucket);
    return vsetBucketFromSingle(entry);
}

static inline vsetBucket *insertToBucket_SINGLE(vsetGetExpiryFunc getExpiry, vsetBucket *bucket, void *entry, long long expiry) {
    /* Upgrade to vector */
    pVector *sv = pvNew(2);
    void *curr_entry = vsetBucketSingle(bucket);
    long long curr_expiry = getExpiry(curr_entry);
    if (curr_expiry < expiry) {
        sv = pvInsert(sv, curr_entry, 0);
        sv = pvInsert(sv, entry, 1);
    } else {
        sv = pvInsert(sv, entry, 0);
        sv = pvInsert(sv, curr_entry, 1);
    }
    bucket = vsetBucketFromVector(sv);
    return bucket;
}

static inline vsetBucket *insertToBucket_VECTOR(vsetGetExpiryFunc getExpiry, vsetBucket *bucket, void *entry, long long expiry) {
    pVector *pv = vsetBucketVector(bucket);
    /* limit of the number of elements in a vector. */
    if (pvLen(pv) >= VOLATILESET_VECTOR_BUCKET_MAX_SIZE) {
        //  Upgrade to hashtable
        hashtable *ht = hashtableCreate(&pointerHashtableType);
        for (uint32_t i = 0; i < pvLen(pv); i++) {
            hashtableAdd(ht, pvGet(pv, i));
        }
        pvFree(pv);
        /* Add the new entry as well */
        hashtableAdd(ht, entry);

        return vsetBucketFromHashtable(ht);
    } else {
        uint32_t pos = findInsertPosition(getExpiry, bucket, expiry);
        return vsetBucketFromVector(pvInsert(pv, entry, pos));
    }
    return NULL;
}

static inline vsetBucket *insertToBucket_HASHTABLE(vsetGetExpiryFunc getExpiry, vsetBucket *bucket, void *entry, long long expiry) {
    UNUSED(getExpiry);
    UNUSED(expiry);

    hashtable *ht = vsetBucketHashtable(bucket);
    assert(hashtableAdd(ht, entry));
    return bucket;
}

static inline vsetBucket *insertToBucket_RAX(vsetGetExpiryFunc getExpiry, vsetBucket *target, void *entry, long long expiry) {
    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    size_t key_len;
    long long bucket_ts;
    rax *expiry_buckets = vsetBucketRax(target);
    raxNode *node;
    vsetBucket *bucket = findBucket(expiry_buckets, expiry, key, &key_len, &bucket_ts, &node);
    int type = vsetBucketType(bucket);
    if (type == VSET_BUCKET_NONE) {
        /* No bucket: create single-entry bucket */
        bucket = insertToBucket_NONE(getExpiry, bucket, entry, expiry);
        assert(vsetBucketType(bucket) == VSET_BUCKET_SINGLE);
        size_t key_size = encodeNewExpiryBucketKey(key, expiry);
        raxInsert(expiry_buckets, key, key_size, bucket, NULL);
        return target;
    } else if (type == VSET_BUCKET_SINGLE) {
        /* Upgrade to vector */
        bucket = insertToBucket_SINGLE(getExpiry, bucket, entry, expiry);
        assert(vsetBucketType(bucket) == VSET_BUCKET_VECTOR);
        /* In order to avoid rax override, we directly change the node data */
        // alternative: raxInsert(expiry_buckets, key, key_len, bucket, NULL);
        raxSetData(node, bucket);
    } else if (type == VSET_BUCKET_VECTOR) {
        pVector *sv = vsetBucketVector(bucket);
        if (pvLen(sv) == VOLATILESET_VECTOR_BUCKET_MAX_SIZE) {
            /* Try to split the bucket. If not possible switch to hashtable encoding. */
            if (!splitBucketIfPossible(target, getExpiry, bucket, bucket_ts, node)) {
                /* Can't split? insrt to the vector anyway, it will just expand to hashtable */
                bucket = insertToBucket_VECTOR(getExpiry, bucket, entry, expiry);
                assert(vsetBucketType(bucket) == VSET_BUCKET_HT);
                /* In order to avoid rax override, we directly change the node data */
                // alternative raxInsert(expiry_buckets, key, key_len, bucket, NULL);
                raxSetData(node, bucket);
            } else {
                /* we split the bucket. go and find again a bucket to place the entry since there can be new options now. */
                return insertToBucket_RAX(getExpiry, target, entry, expiry);
            }
        } else {
            vsetBucket *new_bucket = insertToBucket_VECTOR(getExpiry, bucket, entry, expiry);
            if (new_bucket != bucket)
                /* In order to avoid rax override, we directly change the node data */
                // alternative: raxInsert(expiry_buckets, key, key_len, new_bucket, NULL);
                raxSetData(node, new_bucket);
        }
    } else if (vsetBucketType(bucket) == VSET_BUCKET_HT) {
        bucket = insertToBucket_HASHTABLE(getExpiry, bucket, entry, expiry);
    } else {
        panic("Unknown bucket type in insertToBucket_RAX");
    }
    return target;
}

static inline vsetBucket *removeFromBucket_SINGLE(vsetGetExpiryFunc getExpiry, vsetBucket *bucket, void *entry, long long expiry, bool *removed) {
    UNUSED(getExpiry);
    UNUSED(expiry);

    if (vsetBucketSingle(bucket) == entry) {
        *removed = true;
        return vsetBucketFromNone();
    } else {
        *removed = false;
        return bucket;
    }
}

static inline vsetBucket *removeFromBucket_VECTOR(vsetGetExpiryFunc getExpiry, vsetBucket *bucket, void *entry, long long expiry, bool *removed) {
    UNUSED(getExpiry);
    UNUSED(expiry);

    vsetBucket *new_bucket = bucket;
    bool success = false;
    pVector *sv = vsetBucketVector(bucket);
    /* In case we we removed the entry */
    uint32_t vlen = pvLen(sv);
    if (vlen <= 2) {
        /* convert to single if needed */
        uint32_t idx = pvFind(sv, entry);
        if (idx == vlen) {
            success = false;
        } else {
            if (vlen == 1)
                new_bucket = vsetBucketFromNone();
            else
                new_bucket = vsetBucketFromSingle(pvGet(sv, idx == 0 ? 1 : 0));
            success = true;
            pvFree(sv);
        }
    } else {
        if (pvRemove(&sv, entry)) {
            success = true;
            new_bucket = vsetBucketFromVector(sv);
        }
    }
    if (removed) *removed = success;
    return new_bucket;
}

static inline vsetBucket *removeFromBucket_HASHTABLE(vsetGetExpiryFunc getExpiry, vsetBucket *bucket, void *entry, long long expiry, bool *removed) {
    UNUSED(getExpiry);
    UNUSED(expiry);

    bool success = false;
    vsetBucket *new_bucket = bucket;
    hashtable *ht = vsetBucketHashtable(bucket);
    if (hashtableDelete(ht, entry)) {
        success = true;
        assert(hashtableSize(ht) > 0);
        if (hashtableSize(ht) == 1) {
            // Downgrade to SINGLE
            hashtableIterator hi;
            hashtableInitIterator(&hi, ht, 0);
            void *ptr;
            hashtableNext(&hi, &ptr);
            hashtableRelease(ht);
            new_bucket = vsetBucketFromSingle(ptr);
        }
    }
    if (removed) *removed = success;
    return new_bucket;
}
static bool removeEntryBucketFromRaxBucket(vsetBucket *rax_bucket, vsetGetExpiryFunc getExpiry, void *entry, vsetBucket *bucket, unsigned char *key, size_t key_len, vsetBucket **pbucket, raxNode *node) {
    bool removed = false;
    switch (vsetBucketType(bucket)) {
    case VSET_BUCKET_SINGLE:
        bucket = removeFromBucket_SINGLE(getExpiry, bucket, entry, 0, &removed);
        if (removed) {
            raxRemove(vsetBucketRax(rax_bucket), key, key_len, NULL);
            if (pbucket) *pbucket = NULL;
        }
        break;
    case VSET_BUCKET_VECTOR: {
        vsetBucket *new_bucket = removeFromBucket_VECTOR(getExpiry, bucket, entry, 0, &removed);
        if (new_bucket != bucket) {
            if (!new_bucket) {
                raxRemove(vsetBucketRax(rax_bucket), key, key_len, NULL);
                if (pbucket) *pbucket = NULL;
            } else {
                /* In order to avoid rax override, we directly change the node data */
                // alternative: raxInsert(*set, key, key_len, new_bucket, NULL);
                raxSetData(node, new_bucket);
                if (pbucket) *pbucket = new_bucket;
            }
        }
        break;
    }
    case VSET_BUCKET_HT: {
        vsetBucket *new_bucket = removeFromBucket_HASHTABLE(getExpiry, bucket, entry, 0, &removed);
        if (new_bucket != bucket)
            /* In order to avoid rax override, we directly change the node data */
            // alternative: raxInsert(*set, key, key_len, bucket, NULL);
            raxSetData(node, new_bucket);

        if (pbucket) *pbucket = new_bucket;
        break;
    }
    default:
        panic("Unknown bucket type for removeEntryBucketFromRaxBucket");
        return false;
    }
    return removed;
}

static inline vsetBucket *removeFromBucket_RAX(vsetGetExpiryFunc getExpiry, vsetBucket *target, void *entry, long long expiry, bool *removed) {
    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    long long bucket_ts;
    size_t key_len;
    raxNode *node;
    rax *expiry_buckets = vsetBucketRax(target);
    vsetBucket *bucket = findBucket(expiry_buckets, expiry, key, &key_len, &bucket_ts, &node);
    assert(bucket);
    bool success = removeEntryBucketFromRaxBucket(target, getExpiry, entry, bucket, key, key_len, NULL, node);
    if (removed) *removed = success;
    // shrink to single bucket if possible
    if (raxSize(expiry_buckets) == 1) {
        raxIterator it;
        raxStart(&it, expiry_buckets);
        assert(raxSeek(&it, "^", NULL, 0));
        assert(raxNext(&it));
        bucket = it.data;
        int bucket_type = vsetBucketType(bucket);
        raxStop(&it);
        /* We will not convert hashtable to our only bucket since we will lose the ability to scan the items in a sorted way.
         * We will also not shrink when we have a full vector, since it might immediately be repopulated.  */
        if (bucket_type == VSET_BUCKET_SINGLE ||
            (bucket_type == VSET_BUCKET_VECTOR && pvLen(vsetBucketVector(bucket)) < VOLATILESET_VECTOR_BUCKET_MAX_SIZE)) {
            /* lets make our bucket to be the only left bucket */
            target = bucket;
            raxFree(expiry_buckets);
        }
    }
    return target;
}

static int vsetBucketNext_NONE(vsetIterator *it, void **entryptr) {
    UNUSED(it);
    UNUSED(entryptr);
    return 0;
}
static inline int vsetBucketNext_SINGLE(vsetIterator *it, void **entryptr) {
    bool init_bucket_scan = (it->iteration_state == VSET_BUCKET_NONE);
    if (init_bucket_scan) {
        it->iteration_state = VSET_BUCKET_SINGLE;
        it->entry = vsetBucketSingle(it->bucket);
        if (entryptr) *entryptr = it->entry;
        return 1;
    }
    return 0;
}
static inline int vsetBucketNext_VECTOR(vsetIterator *it, void **entryptr) {
    bool init_bucket_scan = (it->iteration_state == VSET_BUCKET_NONE);
    pVector *pv = vsetBucketVector(it->bucket);
    if (init_bucket_scan) {
        it->iteration_state = VSET_BUCKET_VECTOR;
        it->viter = 0;
    } else {
        it->viter++;
    }
    if (it->viter < pvLen(pv)) {
        it->entry = pvGet(pv, it->viter);
    } else {
        return 0;
    }
    if (entryptr) *entryptr = it->entry;
    return 1;
}

static inline int vsetBucketNext_HASHTABLE(vsetIterator *it, void **entryptr) {
    bool init_bucket_scan = (it->iteration_state == VSET_BUCKET_NONE);
    hashtable *ht = vsetBucketHashtable(it->bucket);
    if (init_bucket_scan) {
        it->iteration_state = VSET_BUCKET_HT;
        hashtableInitIterator(&it->hiter, ht, 0);
    }
    if (!hashtableNext(&it->hiter, &it->entry)) {
        hashtableResetIterator(&it->hiter);
        return 0;
    }
    if (entryptr) *entryptr = it->entry;
    return 1;
}

static inline int vsetBucketNext_RAX(vsetIterator *it, void **entryptr) {
    bool init_bucket_scan = (it->iteration_state == VSET_BUCKET_NONE);
    if (init_bucket_scan) {
        /* set myself as the parent bucket */
        it->parent_bucket = it->bucket;
        raxStart(&it->riter, vsetBucketRax(it->bucket));
        raxSeek(&it->riter, "^", NULL, 0);
    }
    if (raxNext(&it->riter)) {
        /* lets start again by going into the first bucket. */
        it->iteration_state = vsetBucketType(it->riter.data);
        it->bucket_ts = decodeExpiryKey(it->riter.key);
        it->bucket = it->riter.data;
        it->iteration_state = VSET_BUCKET_NONE;
        return vsetNext(it, entryptr);
    } else {
        /* We currently do not support nested RAX buckets */
        it->parent_bucket = vsetBucketFromNone();
        return 0;
    }
    return 1;
}

bool vsetAddEntry(vset *set, vsetGetExpiryFunc getExpiry, void *entry) {
    long long expiry = getExpiry(entry);
    vsetBucket *expiry_buckets = *set;
    int bucket_type = vsetBucketType(expiry_buckets);
    switch (bucket_type) {
    case VSET_BUCKET_NONE:
        expiry_buckets = insertToBucket_NONE(getExpiry, expiry_buckets, entry, expiry);
        break;
    case VSET_BUCKET_SINGLE:
        expiry_buckets = insertToBucket_SINGLE(getExpiry, expiry_buckets, entry, expiry);
        break;
    case VSET_BUCKET_VECTOR: {
        pVector *vec = vsetBucketVector(expiry_buckets);
        uint32_t len = pvLen(vec);
        /* in case the vector is full, we need to turn into RAX */
        if (len == VOLATILESET_VECTOR_BUCKET_MAX_SIZE) {
            rax *r = raxNew();
            long long min_expiry = getExpiry(pvGet(vec, 0));
            long long max_expiry = getExpiry(pvGet(vec, len - 1));
            if (get_max_bucket_ts(min_expiry) == get_max_bucket_ts(max_expiry)) {
                /* In case we can just insert the bucket, no need to iterate and insert it's elements. we can just push the bucket as a whole. */
                unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
                size_t key_len = encodeNewExpiryBucketKey(key, max_expiry);
                raxInsert(r, key, key_len, expiry_buckets, NULL);
                expiry_buckets = vsetBucketFromRax(r);
                expiry_buckets = insertToBucket_RAX(getExpiry, expiry_buckets, entry, expiry);
            } else {
                /* We need to migrate entries to the new set of buckets since we do not know all entries are in the same bucket */
                expiry_buckets = vsetBucketFromRax(r);
                for (uint32_t i = 0; i < len; i++) {
                    void *moved_entry = pvGet(vec, i);
                    expiry_buckets = insertToBucket_RAX(getExpiry, expiry_buckets, moved_entry, getExpiry(moved_entry));
                }
                /* free the vector */
                pvFree(vec);
                /* now insert the new entry to the buckets */
                expiry_buckets = insertToBucket_RAX(getExpiry, expiry_buckets, entry, expiry);
            }
        } else {
            expiry_buckets = insertToBucket_VECTOR(getExpiry, expiry_buckets, entry, expiry);
        }
        break;
    }
    case VSET_BUCKET_RAX:
        expiry_buckets = insertToBucket_RAX(getExpiry, expiry_buckets, entry, expiry);
        break;
    default:
        panic("Cannot insert to bucket which is not single, vector or rax");
    }
    /* update the set */
    *set = expiry_buckets;
    return true;
}

static inline bool vsetRemoveEntryWithExpiry(vset *set, vsetGetExpiryFunc getExpiry, void *entry, long long expiry) {
    bool removed;
    vsetBucket *bucket = *set;
    int bucket_type = vsetBucketType(bucket);
    switch (bucket_type) {
    case VSET_BUCKET_NONE:
        /* We cannot remove from empty set */
        return 0;
    case VSET_BUCKET_SINGLE:
        bucket = removeFromBucket_SINGLE(getExpiry, bucket, entry, expiry, &removed);
        break;
    case VSET_BUCKET_VECTOR:
        bucket = removeFromBucket_VECTOR(getExpiry, bucket, entry, expiry, &removed);
        break;
    case VSET_BUCKET_HT:
        bucket = removeFromBucket_HASHTABLE(getExpiry, bucket, entry, expiry, &removed);
        break;
    case VSET_BUCKET_RAX:
        bucket = removeFromBucket_RAX(getExpiry, bucket, entry, expiry, &removed);
        break;
    default:
        panic("Cannot remove from bucket which is not single, vector, hashtable or rax");
    }
    *set = bucket;
    return removed;
}

bool vsetRemoveEntry(vset *set, vsetGetExpiryFunc getExpiry, void *entry) {
    return vsetRemoveEntryWithExpiry(set, getExpiry, entry, getExpiry(entry));
}

bool vsetUpdateEntry(vset *set, vsetGetExpiryFunc getExpiry, void *old_entry, void *new_entry, long long old_expiry, long long new_expiry) {
    /* Nothing to do */
    if (old_entry == new_entry && old_expiry == new_expiry)
        return true;

    if (old_entry && old_expiry != -1)
        /* We cannot take the expiration time from the removed entry, since it might not be allocated anymore.
         * For this reason we ask the API user to provide us the removed entry expiration time. */
        assert((vsetRemoveEntryWithExpiry(set, getExpiry, old_entry, old_expiry)));

    if (new_entry && new_expiry != -1)
        assert(vsetAddEntry(set, getExpiry, new_entry));

    return true;
}

static void *vsetGetFirstExpired(vset *set, vsetGetExpiryFunc getExpiry, mstime_t now, bool delete) {
    int set_type = vsetBucketType(*set);
    void *entry = NULL;
    long long expiry;
    switch (set_type) {
    case VSET_BUCKET_NONE:
        return NULL;
        break;
    case VSET_BUCKET_RAX: {
        vsetIterator iter;
        vsetStart(set, &iter);
        assert(vsetBucketNext_RAX(&iter, &entry));
        long long bucket_ts = iter.bucket_ts;
        vsetStop(&iter);
        if (bucket_ts > now)
            return NULL;
        expiry = getExpiry(entry);
        assert(expiry <= now);
        break;
    }
    case VSET_BUCKET_SINGLE: {
        entry = vsetBucketSingle(*set);
        expiry = getExpiry(entry);
        if (expiry > now)
            return NULL;
        break;
    }
    case VSET_BUCKET_VECTOR: {
        entry = pvGet(vsetBucketVector(*set), 0);
        expiry = getExpiry(entry);
        if (expiry > now)
            return NULL;
        break;
    }
    case VSET_BUCKET_HT: {
        hashtableIterator iter;
        hashtableInitIterator(&iter, vsetBucketHashtable(*set), 0);
        assert(hashtableNext(&iter, &entry));
        hashtableResetIterator(&iter);
        expiry = getExpiry(entry);
        if (expiry > now)
            return NULL;
        break;
    }
    default:
        panic("Unknown volatile set bucket type in vsetNext");
    }
    if (delete)
        assert(vsetRemoveEntry(set, getExpiry, entry));
    return entry;
}

void *vsetPopExpired(vset *set, vsetGetExpiryFunc getExpiry, mstime_t now) {
    return vsetGetFirstExpired(set, getExpiry, now, true);
}

void *vsetFirstExpired(vset *set, vsetGetExpiryFunc getExpiry, mstime_t now) {
    return vsetGetFirstExpired(set, getExpiry, now, false);
}

bool vsetNext(vsetIterator *it, void **entryptr) {
    vsetBucket *bucket = it->bucket;
    int bucket_type = vsetBucketType(bucket);
    int ret = 0;
    switch (bucket_type) {
    case VSET_BUCKET_NONE:
        return vsetBucketNext_NONE(it, entryptr);
        break;
    case VSET_BUCKET_RAX:
        return vsetBucketNext_RAX(it, entryptr);
        break;
    case VSET_BUCKET_SINGLE:
        ret = vsetBucketNext_SINGLE(it, entryptr);
        break;
    case VSET_BUCKET_VECTOR:
        ret = vsetBucketNext_VECTOR(it, entryptr);
        break;
    case VSET_BUCKET_HT:
        ret = vsetBucketNext_HASHTABLE(it, entryptr);
        break;
    default:
        panic("Unknown volatile set bucket type in vsetNext");
    }
    if (ret == 0) {
        /* continue iterating the parent bucket */
        it->iteration_state = vsetBucketType(it->parent_bucket);
        it->bucket = it->parent_bucket;
        return vsetNext(it, entryptr);
    }
    return ret == 1;
}

void vsetStart(vset *set, vsetIterator *it) {
    it->iteration_state = VSET_BUCKET_NONE; /*lets start by going to the first bucket. */
    it->bucket = *set;
    it->bucket_ts = -1;
    it->parent_bucket = vsetBucketFromNone();
}

void vsetStop(vsetIterator *it) {
    int bucket_type = vsetBucketType(it->bucket);
    int parent_bucket_type = vsetBucketType(it->parent_bucket);
    if (parent_bucket_type == VSET_BUCKET_RAX)
        raxStop(&it->riter);
    if (bucket_type == VSET_BUCKET_HT)
        hashtableResetIterator(&it->hiter);
}

void vsetInit(vset *set) {
    *set = vsetBucketFromNone();
}

/* Free all the vset memory used in order to reference the entries.
 * Since the set only holds references to entries the entries themselves are NOT freed */
void vsetClear(vset *set) {
    if (!(*set)) return;
    freeVsetBucket(*set);
    *set = vsetBucketFromNone();
}

bool vsetIsEmpty(vset *set) {
    return vsetBucketType(*set) == VSET_BUCKET_NONE;
}
