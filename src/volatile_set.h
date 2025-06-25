#ifndef VOLATILESET_H
#define VOLATILESET_H

#include <stddef.h>
#include <stdbool.h>
#include "hashtable.h"

#include "hashtable.h"
#include "rax.h"
#include "sds.h"
#include "monotonic.h" /* for mstime_t*/

/*-----------------------------------------------------------------------------
 * Volatile Set - Time-Bucketed Entry Set
 *
 * The `volatile_set` data structure provides an efficient, time-aware mechanism
 * to track and expire entries based on UNIX timestamps. It is optimized for
 * memory efficiency and fast iteration, particularly for use cases where
 * entries have short-lived expirations in the order of milliseconds.
 *
 * --------------------------------------------------------------------------
 * Overview
 * --------------------------------------------------------------------------
 * The core of the structure is a radix tree (`rax`) where each key represents
 * the end timestamp of a fixed-duration time window (a "bucket"). The value
 * stored for each rax entry is a tagged pointer identifying the type of the
 * bucket and its storage container.
 *
 * Time windows are grouped in variable resolutions:
 *  - Minimum window: VOLATILESET_BUCKET_INTERVAL_MIN (16 milliseconds)
 *  - Maximum window: VOLATILESET_BUCKET_INTERVAL_MAX (8192 milliseconds)
 * The actual granularity used to group entries is defined by
 * VOLATILESET_BUCKET_GRANULARITY.
 *
 * --------------------------------------------------------------------------
 * Entry Pointer Requirements
 * --------------------------------------------------------------------------
 * Entries added to a `volatile_set` must be memory pointers with the least
 * significant bit (LSB) set — that is, *odd-addressed pointers*. This is
 * necessary for internal tagging logic. Common use cases, like `sds`, already
 * meet this requirement.
 *
 * --------------------------------------------------------------------------
 * Bucket Types
 * --------------------------------------------------------------------------
 * Buckets in the rax are implemented as tagged pointers supporting one of the
 * following types:
 *
 * 1. SINGLE  (Tag: xx1)
 *    - Stores a single entry directly in the rax value.
 *    - Used when there is only one element in a time bucket.
 *
 * 2. VECTOR  (Tag: x00)
 *    - Stores a pointer to a `pointer_vector` (memory-efficient dynamic array).
 *    - Used when multiple entries fall within the same bucket window.
 *    - Efficient for small bucket sizes (up to ~127 entries).
 *    - Maintains entries in sorted order by expiry using binary search for insertions.
 *
 * 3. HASHTABLE (Tag: x10)
 *    - Stores a pointer to a hashtable of entries.
 *    - Used when a vector overflows or entries cannot be further split due to
 *      coarse timestamp resolution.
 *    - Supports fast random lookup and deletion.
 *
 * 4. NONE (Tag: NULL)
 *    - Represents an uninitialized or cleared bucket.
 *
 * --------------------------------------------------------------------------
 * Bucket Resolution and Organization
 * --------------------------------------------------------------------------
 *
 * Volatile Set RAX Structure with Tagged Bucket Types
 *
 *      +---------------------------------------+
 *      |               RAX                     |
 *      +---------------------------------------+
 *                 /         |           \
 *        +-----------+   +----------+  +------------+
 *        | Key: 1232 |   | Key: 1248|  | Key: 8192  |
 *        +-----------+   +----------+  +------------+
 *        | Bucket: * |   | Bucket: *|  | Bucket: *  |
 *        |  Single   |   |  Vector  |  |  HashTbl   |
 *        +-----------+   +----------+  +------------+
 *
 * Bucket Types:
 *
 * 1. SINGLE (Tagged LSB == 1):
 *    +-----------------------+
 *    | entry (odd pointer)  |
 *    +-----------------------+
 *
 * 2. VECTOR (Tag == x00 binary):
 *    +------------------------+
 *    | pointer_vector *      |
 *    |  [entry1, entry2, ...]|
 *    +------------------------+
 *
 * 3. HASHTABLE (Tag == x10 binary):
 *    +------------------------+
 *    | hashtable *           |
 *    |  {"field1": entry1}   |
 *    |  {"field2": entry2}   |
 *    +------------------------+
 *
 * Notes:
 * - All keys in the RAX are timestamps (as `long long`) indicating the
 *   end of the time window for a bucket.
 * - Entries must be pointers with the **LSB bit set** (odd memory addresses),
 *   which is true for SDS strings.
 * - Buckets grow from SINGLE → VECTOR → HASHTABLE based on size and distribution.
 * - Removal may shrink HASHTABLE/VECTOR back to VECTOR/SINGLE if small.
 *
 * --------------------------------------------------------------------------
 * Insertion
 * --------------------------------------------------------------------------
 * Each rax key corresponds to a bucket end timestamp. When a new entry is
 * added, the set searches for the first bucket whose timestamp is greater
 * than the entry's expiration. If:
 *
 *  - No such bucket exists, or
 *  - The entry does not fit within the bucket’s window,
 *
 * ...a new bucket is created. Initially, it is of SINGLE type. Buckets are
 * promoted as they grow:
 *
 *      SINGLE → VECTOR → HASHTABLE
 *
 * When inserting into a VECTOR bucket:
 *  - The vector is kept sorted by expiration timestamp.
 *  - If full, an attempt is made to split the vector into two balanced ones.
 *  - If all timestamps are too similar to split, the bucket is promoted to a
 *    HASHTABLE.
 *
 * --------------------------------------------------------------------------
 * Removal and Downgrade
 * --------------------------------------------------------------------------
 * Removing an entry:
 *  - Locates the appropriate bucket by searching for the first bucket whose
 *    timestamp is greater than the entry’s expiration.
 *  - Removes the entry from the bucket.
 *  - If the bucket shrinks to a single entry, it is downgraded back to SINGLE.
 *
 * --------------------------------------------------------------------------
 * Advantages
 * --------------------------------------------------------------------------
 * - Memory-efficient storage using tagged pointers and dynamic structure scaling.
 * - Fast time-based lookup, insertion, and expiration.
 * - Efficient iteration using radix tree ordering.
 * - Well-suited for time-sensitive caches, ephemeral data, or short-lived sets.
 *
 * --------------------------------------------------------------------------
 * Example Bucket Memory Layouts
 * --------------------------------------------------------------------------
 * [SINGLE]
 * rax: key = 1234567890
 *      value = <entry pointer with LSB=1>
 *
 * [VECTOR]
 * rax: key = 1234567910
 *      value = (tagged pointer) → pointer_vector of entry pointers
 *
 * [HASHTABLE]
 * rax: key = 1234568000
 *      value = (tagged pointer) → hashtable of entry pointers
 *
 * --------------------------------------------------------------------------
 * Notes
 * --------------------------------------------------------------------------
 * - All expiration timestamps are UNIX milliseconds.
 * - All entry pointers **must** be odd-addressed (LSB = 1).
 * - Buckets are dynamically upgraded or downgraded based on size.
 * - Iteration and expiration use efficient radix-based access patterns.
 *
 * --------------------------------------------------------------------------
 * See Also:
 * volatileSetRemoveEntry()
 * volatileSetAddEntry()
 * volatileSetUpdateEntry()
 * volatileSetdPopExpired()
 * volatileSetFirstExpired()
 * volatileSetIsEmpty(volatile_set *set)
 * volatileSetStart()
 * volatileSetNext()
 * volatileSetReset()
 * freeVolatileSet()
 * createVolatileSet()
 * - `pointer_vector`, `hashtable`, and tagged pointer APIs.
 *----------------------------------------------------------------------------*/

#define VOLATILESET_BUCKET_INTERVAL_MAX (1LL << 13LL) // 2^13 = 8192 milliseconds
#define VOLATILESET_BUCKET_INTERVAL_MIN (1LL << 4LL)  // 2^4 = 16 milliseconds

#define VOLATILESET_VECTOR_BUCKET_MAX_SIZE 127
typedef struct {
    sds (*entryGetKey)(const void *entry);

    long long (*getExpiry)(const void *entry);

    int (*expire)(void*db, void* o, void *entry);

} volatileEntryType;


typedef struct {
    volatileEntryType *etypr;
    rax *expiry_buckets;
} volatile_set;

typedef struct volatileSetIterator {
    raxIterator bucket;
    /* Different bucket iterator types */
    hashtableIterator hiter;
    uint32_t viter;
    void *entry;
    long long bucket_ts;
    //volatile_set *set;
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
