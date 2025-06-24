#ifndef VOLATILESET_H
#define VOLATILESET_H

#include <stddef.h>
#include <stdbool.h>
#include "hashtable.h"

#include "hashtable.h"
#include "rax.h"
#include "sds.h"
#include "monotonic.h" /* for mstime_t*/

#define VOLATILESET_BUCKET_INTERVAL_MAX (1LL << 13LL) // 2^13 = 8192 milliseconds
#define VOLATILESET_BUCKET_INTERVAL_MIN (1LL << 4LL)  // 2^4 = 16 milliseconds
#define VOLATILESET_BUCKET_GRANULARITY VOLATILESET_BUCKET_INTERVAL

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
int volatileSetExpireEntry(volatile_set *set, volatileSetIterator *it, mstime_t now, void *serverDb, void *o);
int volatileSetUpdateEntry(volatile_set *set, void *old_entry, void *new_entry, long long old_expiry, long long new_expiry);
bool volatileSetIsEmpty(volatile_set *set);
void volatileSetStart(volatile_set *set, volatileSetIterator *it);
int volatileSetNext(volatileSetIterator *it, void **entryptr);
void volatileSetReset(volatileSetIterator *it);
void freeVolatileSet(volatile_set *b);
volatile_set *createVolatileSet(volatileEntryType *type);


#endif
