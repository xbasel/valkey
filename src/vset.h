#ifndef VOLATILESET_H
#define VOLATILESET_H

#include <stddef.h>
#include <stdbool.h>

#include "hashtable.h"
#include "rax.h"
#include "sds.h"
#include "monotonic.h" /* for mstime_t*/

/*
 *-----------------------------------------------------------------------------
 * Volatile Set - Adaptive, Expiry-aware Set Structure
 *-----------------------------------------------------------------------------
 *
 * The `vset` is a dynamic, memory-efficient container for managing
 * entries with expiry semantics. It is designed to efficiently track entries
 * that expire at varying times and scales to large sets by adapting its internal
 * representation as it grows or shrinks.
 *
 *-----------------------------------------------------------------------------
 * Expiry Buckets and Pointer Tagging
 *-----------------------------------------------------------------------------
 *
 * Internally, the `vset` maintains a single `vsetBucket*` pointer,
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
 *      -> it is promoted to a `VECTOR` bucket (sorted by expiry).
 * 3. If the `VECTOR` exceeds `VOLATILESET_VECTOR_BUCKET_MAX_SIZE` (127):
 *      -> the set becomes a `RAX`, and existing entries are migrated.
 * 4. IF the set is using RAX encoding it will locate a bucket to add the entry
 *    following the strategy explained below.
 *
 *-----------------------------------------------------------------------------
 * RAX Bucket and Dynamic Splitting
 *-----------------------------------------------------------------------------
 *
 * Each bucket in the RAX bucket corresponds to a **time window**, defined by
 * its bucket timestamp (`bucket_ts`). This timestamp represents the **END** of
 * the time window. Entries in the bucket must expire *before* this timestamp.
 *
 * Time windows are defined in granular ranges:
 *   - Minimum granularity: VOLATILESET_BUCKET_INTERVAL_MIN (16 ms)
 *   - Maximum granularity: VOLATILESET_BUCKET_INTERVAL_MAX (8192 ms)
 *
 * A bucket can only contain entries that:
 *   1. Have expiry < bucket_ts
 *   2. Do not fit into any bucket with a smaller timestamp (i.e., earlier window)
 *
 * The structure allows multiple encodings:
 *     VSET_BUCKET_SINGLE  - A single pointer to one entry.
 *     VSET_BUCKET_VECTOR  - A sorted vector of pointers (up to 127 entries).
 *     VSET_BUCKET_HT      - A hashtable used when vectors become too dense.
 *
 * Bucket Timestamp (END of window):
 *
 *        |------------------ Bucket Span ------------------|
 *        [window_start .................................. bucket_ts)
 *
 * Layout Example:
 *
 *   Timeline:         ----------> increasing time ----------->
 *                     +--------------+-------------+---------+
 *                     | B0           | B1          |   B2    |
 *                     | ts=32        | ts=128      | ts=2048 |
 *                     +--------------+-------------+---------+
 *                     ^              ^             ^
 *                     |              |             |
 *           [E1,E2] ∈ B0      [E3...E7] ∈ B1     [E8...E15] ∈ B2
 *
 *           All entries expire BEFORE their bucket_ts
 *
 * Bucket Splitting Strategy:
 * ----------------------------------
 *
 * When a bucket (e.g. VECTOR) becomes too dense or needs realignment:
 *
 * 1. Re-align to lower granularity:
 *      - Adjust the bucket timestamp down to a finer granularity (e.g. 16ms).
 *      - Only done if ALL entries still fit in the tighter window.
 *      - Effectively “moves” the bucket to an earlier timestamp.
 *
 *        Example: B(ts=128, span=128ms) -> B(ts=64, span=16ms)
 *
 * 2. Split into two buckets:
 *      - Use binary search to find a “natural” boundary based on entry expiry.
 *      - Original bucket retains its timestamp (but holds fewer entries).
 *      - New bucket is inserted before the current one with its own tighter timestamp.
 *
 *        Example:
 *
 *        Before:
 *             [ Entry0 ... Entry126 ]  -> B(ts=128)
 *
 *        After Split:
 *             [ Entry0...Entry62 ]     -> New B(ts=64)
 *             [ Entry63...Entry126 ]   -> Original B(ts=128)
 *
 * 3. Convert to hashtable:
 *      - When no clean split is found (e.g. all entries share similar expiry),
 *        and realignment is not possible.
 *      - This allows efficient O(1) lookups even with clustered expiry values.
 *
 *        Vector B(ts=128) -> Hashtable B(ts=128)
 *
 * This hierarchical design ensures:
 *   - Efficient memory usage (tight buckets)
 *   - Predictable iteration by expiry time
 *   - Low overhead insertions & deletions
 *   - Graceful promotion & demotion of bucket types
 *
 * NOTE: Buckets are always sorted by their `bucket_ts` in the radix tree (RAX),
 *       which allows efficient search for insertion/removal based on expiry.
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
 *     | RAX (key = bucket_ts)    |
 *     |--------------------------|
 *     | "000016" -> [entry1]     |  <- Vector (SINGLE->VECTOR->HT)
 *     | "000032" -> [entry2...]  |  <- Full vector, might split
 *     | "000048" -> [entry...]   |
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
 *            bucket A -> [entry1..entry6]  with key = "000032"
 *            bucket B -> [entry7..entry13] with key = "000048"
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
 *     RAX (holds multiple buckets, keyed by each bucket's end timestamp)
 *     Bucket types within a RAX:
 *
 *                    SINGLE
 *                      |
 *                      v
 *                    VECTOR (sorted, up to 127, can split
 *                      |     into multiple vectors)
 *                      |
 *                      v
 *                   HASHTABLE (only when a vector can't split)
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
 *     void vsetInit(vset *set);
 *     void vsetClear(vset *set);
 *
 * Mutation:
 *     bool vsetAddEntry(vset *set, vsetGetExpiryFunc getExpiry, void *entry);
 *     bool vsetRemoveEntry(vset *set, vsetGetExpiryFunc getExpiry, void *entry);
 *     bool vsetUpdateEntry(vset *set, vsetGetExpiryFunc getExpiry, void *old_entry,
 *                                void *new_entry, long long old_expiry,
 *                                long long new_expiry);
 *
 * Expiry Retrieval/Removal:
 *     long long vsetEstimatedEarliestExpiry(vset *set, vsetGetExpiryFunc getExpiry);
 *     size_t vsetPopExpired(vset *set, vsetGetExpiryFunc getExpiry, vsetExpiryFunc expiryFunc, mstime_t now, size_t max_count, void *ctx);
 *
 * Utilities:
 *     bool vsetIsEmpty(vset *set);
 *
 * Iteration:
 *     void vsetStart(vset *set, vsetIterator *it);
 *     bool vsetNext(vsetIterator *it, void **entryptr);
 *     void vsetStop(vsetIterator *it);
 *
 *-----------------------------------------------------------------------------
 * Iteration Support
 *-----------------------------------------------------------------------------
 *
 * Iterator structure maintains context across all bucket types:
 *
 *     typedef struct vsetIterator {
 *         raxIterator riter;           // for RAX
 *         hashtableIterator hiter;     // for HT
 *         uint32_t viter;              // for VECTOR
 *         void *vsingle;               // for SINGLE
 *         vsetBucket *parent_bucket;   // owning bucket
 *         vsetBucket *bucket;          // active bucket
 *         void *entry;                 // current entry
 *         long long bucket_ts;         // for RAX
 *         int iteration_state;         // internal FSM
 *     } vsetIterator;
 * */

#define VOLATILESET_BUCKET_INTERVAL_MAX (1LL << 13LL) // 2^13 = 8192 milliseconds
#define VOLATILESET_BUCKET_INTERVAL_MIN (1LL << 4LL)  // 2^4 = 16 milliseconds

#define VOLATILESET_VECTOR_BUCKET_MAX_SIZE 127

typedef long long (*vsetGetExpiryFunc)(const void *entry);
typedef int (*vsetExpiryFunc)(void *entry, void *ctx);

// Generic bucket type
typedef void vsetBucket;

// vset is just a pointer to a bucket
typedef vsetBucket *vset;

typedef struct vsetIterator {
    /* for rax bucket */
    raxIterator riter;
    union {
        /* for hashtable bucket */
        hashtableIterator hiter;
        /* for vector bucket */
        uint32_t viter;
        /* for single bucket */
        void *vsingle;
    };
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
} vsetIterator;

bool vsetAddEntry(vset *set, vsetGetExpiryFunc getExpiry, void *entry);
bool vsetRemoveEntry(vset *set, vsetGetExpiryFunc getExpiry, void *entry);
bool vsetUpdateEntry(vset *set, vsetGetExpiryFunc getExpiry, void *old_entry, void *new_entry, long long old_expiry, long long new_expiry);
bool vsetIsEmpty(vset *set);
void vsetStart(vset *set, vsetIterator *it);
bool vsetNext(vsetIterator *it, void **entryptr);
void vsetStop(vsetIterator *it);
void vsetInit(vset *set);
void vsetClear(vset *set);
long long vsetEstimatedEarliestExpiry(vset *set, vsetGetExpiryFunc getExpiry);
size_t vsetPopExpired(vset *set, vsetGetExpiryFunc getExpiry, vsetExpiryFunc expiryFunc, mstime_t now, size_t max_count, void *ctx);
size_t vsetMemUsage(vset *set);
size_t vsetScanDefrag(vset *set, size_t cursor, void *(*defragfn)(void *), int (*defragRaxNode)(raxNode **));
bool vsetHasRax(vset *set);

#endif
