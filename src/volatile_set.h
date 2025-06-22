#ifndef VOLATILESET_H
#define VOLATILESET_H

#include <stddef.h>

#include "hashtable.h"
#include "rax.h"
#include "sds.h"

#define VOLATILESET_BUCKET_GRANULARITY 60000  // 60s

typedef struct {
    sds (*entryGetKey)(const void *entry);

    long long (*getExpiry)(const void *entry);

    int (*expire)(void *entry);

} volatileEntryType;


typedef struct {
    volatileEntryType *etypr;
    rax *expiry_buckets;
} volatile_set;

#define VSET_BUCKET_SINGLE 0
#define VSET_BUCKET_LISTPACK 1
#define VSET_BUCKET_HT 2

typedef struct {
    int type;
    union {
        void *single;
        void *listpack;
        hashtable *hashtable;
    } data;
} vsetBucket;

typedef struct volatileSetIterator {
    raxIterator bucket;
    vsetBucket *current_bucket;
    int state; // 0 = uninitialized, 1 = single done, 2 = iterating listpack, 3 = iterating hashtable
    void *inner_it;
} volatileSetIterator;

int volatileSetRemoveEntry(volatile_set *set, void *entry, long long expiry);
int volatileSetAddEntry(volatile_set *set, void *entry, long long expiry);
int volatileSetExpireEntry(volatile_set *set, void *entry);
int volatileSetUpdateEntry(volatile_set *set, void *old_entry, void *new_entry, long long old_expiry, long long new_expiry);
size_t volatileSetNumEntries(volatile_set *set);
void volatileSetStart(volatile_set *set, volatileSetIterator *it);
int volatileSetNext(volatileSetIterator *it, void **entryptr);
void volatileSetReset(volatileSetIterator *it);
vsetBucket *volatileSetGetOldestBucketBelow(volatile_set *vs, uint64_t now);
size_t encodeExpiryBucketKey(unsigned char *key, long long expiry);

void freeVolatileSet(volatile_set *b);
volatile_set *createVolatileSet(volatileEntryType *type);

#endif
