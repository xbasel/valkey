#ifndef VOLATILESET_H
#define VOLATILESET_H

#include <stddef.h>
#include <stdbool.h>
#include "hashtable.h"

#include "hashtable.h"
#include "rax.h"
#include "sds.h"
#include "monotonic.h" /* for mstime_t*/

/*
 *-----------------------------------------------------------------------------
 * Volatile Set - Adaptive, Expiry-aware Set Structure
 *-----------------------------------------------------------------------------
 *
 * The `volatile_set` is a dynamic, memory-efficient container for managing
 * entries with expiry semantics. It is designed to efficiently track entries
 * that expire at varying times and scales to large sets by adapting its internal
 * representation as it grows or shrinks.
 *
 *-----------------------------------------------------------------------------
 * Expiry Buckets and Pointer Tagging
 *-----------------------------------------------------------------------------
 *
 * Internally, the `volatile_set` maintains a single `vsetBucket*` pointer,
 * which can point to different types of buckets depending on the number of
 * entries and the needed resolution. The pointer is tagged using the lowest 3 bits:
 *
 *     #define VSET_BUCKET_NONE   -1
 *     #define VSET_BUCKET_SINGLE 0x1ULL  // pointer to single entry (odd ptr)
 *     #define VSET_BUCKET_VECTOR 0x2ULL  // pointer to pointer vector
 *     #define VSET_BUCKET_HT     0x4ULL  // pointer to hashtable
 *     #define VSET_BUCKET_RAX    0x6ULL  // pointer to radix tree
 *
 *     #define VSET_TAG_MASK      0x7ULL
 *     #define VSET_PTR_MASK      (~VSET_TAG_MASK)
 *
 * IMPORTANT!!!! - All entries must have LSB set (i.e., be odd-aligned) to be compatible with !!!!
 * tagging constraints.
 *
 *-----------------------------------------------------------------------------
 * Time Bucket Management
 *-----------------------------------------------------------------------------
 *
 * Entries are grouped into **time buckets** based on their expiry time.
 * Each time bucket represents a window aligned to:
 *
 *     #define VOLATILESET_BUCKET_INTERVAL_MIN  (1 << 4)  // 16ms
 *     #define VOLATILESET_BUCKET_INTERVAL_MAX  (1 << 13) // 8192ms
 *
 * A time bucket key is computed by rounding the expiry timestamp up to the
 * nearest aligned window using `get_bucket_ts()`.
 *
 *-----------------------------------------------------------------------------
 * Entry Addition and Bucket Promotion
 *-----------------------------------------------------------------------------
 *
 * When a new entry is added:
 *
 * 1. If the current set is `NONE`, it becomes a `SINGLE` bucket.
 * 2. If the set is a `SINGLE` bucket and another entry arrives:
 *      → it is promoted to a `VECTOR` bucket (sorted by expiry).
 * 3. If the `VECTOR` exceeds `VOLATILESET_VECTOR_BUCKET_MAX_SIZE` (127):
 *      → the set becomes a `RAX`, and existing entries are migrated.
 *
 *-----------------------------------------------------------------------------
 * RAX Bucket and Dynamic Splitting
 *-----------------------------------------------------------------------------
 *
 * A `VSET_BUCKET_RAX` bucket stores multiple time-aligned buckets in a radix tree.
 * Each key in the RAX represents the **end timestamp** of a bucket window.
 *
 * When a bucket in RAX becomes full (vector limit exceeded):
 * - The vector is split into two parts using a **binary search** to find an optimal
 *   split point where the expiry bucket timestamp changes.
 * - Two new buckets are created and inserted back into the RAX with their new
 *   aligned timestamps as keys.
 * - If entries cannot be split (all in same window), the bucket is promoted to HT.
 *
 *-----------------------------------------------------------------------------
 * RAX Bucket Layout
 *-----------------------------------------------------------------------------
 *
 * * RAX View with Time Keys:
 *
 *     expiry_buckets = rax * | 0x6
 *
 *     +--------------------------+
 *     | RAX (key = bucket_ts)   |
 *     |--------------------------|
 *     | "000016" → [entry1]     |  ← Vector (SINGLE→VECTOR→HT)
 *     | "000032" → [entry2...]  |  ← Full vector, might split
 *     | "000048" → [entry...]   |
 *     +--------------------------+
 *
 * * Splitting a Full Vector in RAX:
 *
 *     Suppose vector at key "000032" has 13 entries:
 *
 *     1. Use binary search to find a transition point in expiry bucket_ts.
 *        We search the first 2 following entries which belong to different lwo granularity time windows,
 *        but as close as possible to the middle of the vector:
 *            [entry1, entry7, ..., entry13]
 *                          ↑
 *                         split (first where get_bucket_ts(entry) > min_ts)
 *
 *     2. Create two vectors:
 *            bucket A → [entry1..entry6]  with key = "000032"
 *            bucket B → [entry7..entry13] with key = "000048"
 *
 *     3. Insert both back to the RAX.
 *
 *-----------------------------------------------------------------------------
 * Bucket Lifecycle
 *-----------------------------------------------------------------------------
 *
 *     NONE
 *       |
 *       v
 *     SINGLE (1 entry)
 *       |
 *       v
 *     VECTOR (sorted, up to 127)
 *       |
 *       v
 *     RAX
 *       |
 *       v
 *     +-------------+
 *     | key → bucket|
 *     +-------------+
 *     | "000016" → VECTOR
 *     | "000032" → HT
 *     | "000048" → SINGLE
 *     +-------------+
 *
 *-----------------------------------------------------------------------------
 * Entry Type Contract
 *-----------------------------------------------------------------------------
 *
 * Users must supply a `volatileEntryType` implementation:
 *
 *     typedef struct {
 *         sds (*entryGetKey)(const void *entry);      // get key
 *         long long (*getExpiry)(const void *entry);  // get expiry
 *         int (*expire)(void *db, void *o, void *entry); // trigger expiry
 *     } volatileEntryType;
 *
 *-----------------------------------------------------------------------------
 * Public API
 *-----------------------------------------------------------------------------
 *
 * Create/Free:
 *     volatile_set *createVolatileSet(volatileEntryType *type);
 *     void freeVolatileSet(volatile_set *set);
 *
 * Mutation:
 *     int volatileSetAddEntry(volatile_set *set, void *entry, long long expiry);
 *     int volatileSetRemoveEntry(volatile_set *set, void *entry, long long expiry);
 *     int volatileSetUpdateEntry(volatile_set *set, void *old_entry,
 *                                void *new_entry, long long old_expiry,
 *                                long long new_expiry);
 *
 * Expiry Retrieval:
 *     void *volatileSetFirstExpired(volatile_set *set, mstime_t now);
 *     void *volatileSetdPopExpired(volatile_set *set, mstime_t now);
 *
 * Utilities:
 *     bool volatileSetIsEmpty(volatile_set *set);
 *
 * Iteration:
 *     void volatileSetStart(volatile_set *set, volatileSetIterator *it);
 *     int volatileSetNext(volatileSetIterator *it, void **entryptr);
 *     void volatileSetReset(volatileSetIterator *it);
 *
 *-----------------------------------------------------------------------------
 * Iteration Support
 *-----------------------------------------------------------------------------
 *
 * Iterator structure maintains context across all bucket types:
 *
 *     typedef struct volatileSetIterator {
 *         raxIterator riter;           // for RAX
 *         hashtableIterator hiter;     // for HT
 *         uint32_t viter;              // for VECTOR
 *         void *vsingle;               // for SINGLE
 *         vsetBucket *parent_bucket;   // owning bucket
 *         vsetBucket *bucket;          // active bucket
 *         void *entry;                 // current entry
 *         long long bucket_ts;         // for RAX
 *         int iteration_state;         // internal FSM
 *     } volatileSetIterator;
 * */

#define VOLATILESET_BUCKET_INTERVAL_MAX (1LL << 13LL) // 2^13 = 8192 milliseconds
#define VOLATILESET_BUCKET_INTERVAL_MIN (1LL << 4LL)  // 2^4 = 16 milliseconds

#define VOLATILESET_VECTOR_BUCKET_MAX_SIZE 127
typedef struct {
    sds (*entryGetKey)(const void *entry);

    long long (*getExpiry)(const void *entry);

    int (*expire)(void*db, void* o, void *entry);

} volatileEntryType;

// Generic bucket type
typedef void vsetBucket;

typedef struct {
    volatileEntryType *etypr;
    vsetBucket *expiry_buckets;
} volatile_set;

typedef struct volatileSetIterator {
    /* for rax bucket */
    raxIterator riter;
    /* for hashtable bucket */
    hashtableIterator hiter;
    /* for vector bucket */
    uint32_t viter;
    /* for single bucket */
    void *vsingle;
    /* the parent of the bucket we are currently iterating on */
    vsetBucket *parent_bucket;
    /* the bucket we are currently iterating on */
    vsetBucket *bucket;
    /* the pointer entry */
    void *entry;
    /* In case of rax encoded set, this is the current iterated bucket timestamp */
    long long bucket_ts;
    /* the state of the iteration */
    int iteration_state; 
} volatileSetIterator;

int volatileSetRemoveEntry(volatile_set *set, void *entry, long long expiry);
int volatileSetAddEntry(volatile_set *set, void *entry, long long expiry);
void *volatileSetdPopExpired(volatile_set *set, mstime_t now);
void *volatileSetFirstExpired(volatile_set *set, mstime_t now);
int volatileSetUpdateEntry(volatile_set *set, void *old_entry, void *new_entry, long long old_expiry, long long new_expiry);
bool volatileSetIsEmpty(volatile_set *set);
void volatileSetStart(volatile_set *set, volatileSetIterator *it);
int volatileSetNext(volatileSetIterator *it, void **entryptr);
void volatileSetReset(volatileSetIterator *it);
void freeVolatileSet(volatile_set *b);
volatile_set *createVolatileSet(volatileEntryType *type);


#endif
