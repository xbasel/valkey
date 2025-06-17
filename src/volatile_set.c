#include <string.h>
#include "volatile_set.h"
#include "zmalloc.h"
#include "config.h"
#include "endianconv.h"
#include "serverassert.h"
#include "hashtable.h"
#include "listpack.h"
#include "server.h"

#define EXPIRY_HASH_SIZE 16
#define VSET_BUCKET_KEY_LEN 8

volatile_set *createVolatileSet(volatileEntryType *type) {
    volatile_set *set = zmalloc(sizeof(volatile_set));
    set->etypr = type;
    set->expiry_buckets = raxNew();
    return set;
}

void freeVolatileSet(volatile_set *b) {
    if (!b) return;

    raxIterator ri;
    raxStart(&ri, b->expiry_buckets);
    raxSeek(&ri, "^", NULL, 0); // Start from smallest

    while (raxNext(&ri)) {
        vsetBucket *bucket = ri.data;
        switch (bucket->type) {
            case VSET_BUCKET_SINGLE:
                // No internal memory to free
                break;
            case VSET_BUCKET_LISTPACK:
                lpFree(bucket->data.listpack);
                break;
            case VSET_BUCKET_HT:
                hashtableRelease(bucket->data.hashtable);
                break;
            default:
                serverPanic("Unknown volatile set type in freeVolatileSet");
        }
        zfree(bucket);
    }

    raxStop(&ri);
    raxFree(b->expiry_buckets);
    zfree(b);
}

size_t encodeExpiryBucketKey(unsigned char *key, long long expiry) {
    long long bucket_ts = (expiry / VOLATILESET_BUCKET_GRANULARITY) * VOLATILESET_BUCKET_GRANULARITY;
    long long be_ts = htonu64(bucket_ts);
    size_t size = sizeof(be_ts);
    memcpy(key, &be_ts, size);
    return size;
}


int volatileSetAddEntry(volatile_set *set, void *entry, long long expiry) {
    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    size_t key_size = encodeExpiryBucketKey(key, expiry);

    vsetBucket *bucket = NULL;
    void *existing = NULL;

    if (!raxFind(set->expiry_buckets, key, key_size, &existing)) {
        // No bucket: create single-entry bucket
        bucket = zmalloc(sizeof(vsetBucket));
        bucket->type = VSET_BUCKET_SINGLE;
        bucket->data.single = entry;
        raxInsert(set->expiry_buckets, key, key_size, bucket, NULL);
        return 1;
    }

    bucket = existing;

    if (bucket->type == VSET_BUCKET_SINGLE) {
        // Upgrade to listpack
        serverAssert(bucket->data.single != entry);
        void *lp = lpNew(0);
        lp = lpAppendInteger(lp, (uintptr_t) bucket->data.single);
        lp = lpAppendInteger(lp, (uintptr_t) entry);
        bucket->type = VSET_BUCKET_LISTPACK;
        bucket->data.listpack = lp;
        return 1;
    }

    if (bucket->type == VSET_BUCKET_LISTPACK) {
        uintptr_t pval = (uintptr_t) entry;
        serverAssert(!lpFind(bucket->data.listpack, lpFirst(bucket->data.listpack), (unsigned char *)pval, sizeof(entry), 0));
        bucket->data.listpack = lpAppendInteger(bucket->data.listpack, pval);
        if (lpLength(bucket->data.listpack) > 128) {
            // Upgrade to hashtable
            hashtable *ht = hashtableCreate(&hashHashtableType);
            unsigned char *p = lpFirst(bucket->data.listpack);
            while (p) {
                unsigned int slen;
                long long val;
                lpGetValue(p, &slen, &val);
                hashtableAdd(ht, (void *) val);
                p = lpNext(bucket->data.listpack, p);
            }
            hashtableAdd(ht, entry);
            lpFree(bucket->data.listpack);
            bucket->type = VSET_BUCKET_HT;
            bucket->data.hashtable = ht;
        }
        return 1;
    }

    if (bucket->type == VSET_BUCKET_HT) {
        serverAssert(!hashtableFind(bucket->data.hashtable, entry, NULL));
        hashtableAdd(bucket->data.hashtable, entry);
        return 1;
    }

    serverPanic("Unknown bucket type");
    return 0;
}


int volatileSetRemoveEntry(volatile_set *set, void *entry, long long expiry) {

    unsigned char key[VSET_BUCKET_KEY_LEN] = {0};
    size_t key_size = encodeExpiryBucketKey(key, expiry);

    void *raw = NULL;
    if (!raxFind(set->expiry_buckets, key, key_size, &raw))
        return 0; // bucket not found

    vsetBucket *bucket = raw;

    switch (bucket->type) {
        case VSET_BUCKET_SINGLE:
            if (bucket->data.single != entry) return 0;
            raxRemove(set->expiry_buckets, key, key_size, NULL);
            zfree(bucket);
            return 1;

        case VSET_BUCKET_LISTPACK: {
            void *lp = bucket->data.listpack;
            if (lpFind(lp, NULL, (unsigned char *)&entry, sizeof(entry), 0)) {
                if (!lpDelete(lp,entry,NULL)) return 0;
            }

            if (lpLength(lp) == 1) {
                // Downgrade back to SINGLE
                unsigned int slen;
                long long val;
                lpGetValue(lp, &slen, &val);
                bucket->type = VSET_BUCKET_SINGLE;
                bucket->data.single = (void *)(uintptr_t)val;
                lpFree(lp);
            } else {
                bucket->data.listpack = lp;
            }
            return 1;
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
            return 1;
    }

    serverPanic("Unknown bucket type in volatileSetRemoveEntry");
    return 0;
}

int volatileSetUpdateEntry(volatile_set *set, void *old_entry, void *new_entry,
                           long long old_expiry, long long new_expiry) {
    if (old_entry == new_entry && old_expiry == new_expiry)
        return 1;

    if (old_entry && old_expiry != -1)
        volatileSetRemoveEntry(set, old_entry, old_expiry);

    if (new_entry && new_expiry != -1)
        volatileSetAddEntry(set, new_entry, new_expiry);

    return 1;
}

int volatileSetExpireEntry(volatile_set *set, void *entry) {
    volatileSetRemoveEntry(set, entry, set->etypr->getExpiry(entry));
    if (set->etypr->expire) {
        set->etypr->expire(entry);
        return 1;
    }
    return 0;
}

size_t volatileSetNumEntries(volatile_set *set) {
    assert(set && set->expiry_buckets);
    return set->expiry_buckets->numele;
}

void volatileSetStart(volatile_set *set, volatileSetIterator *it) {
    raxStart(&it->bucket, set->expiry_buckets);
    raxSeek(&it->bucket, ">=", NULL, 0);
}

int volatileSetNext(volatileSetIterator *it, void **entryptr) {
    if (raxNext(&it->bucket)) {
        // assert(it->bucket.key_len != EXPIRY_HASH_SIZE);
        // memcpy(it->bucket.key + 8, entryptr, sizeof(*entryptr));
        *entryptr = it->bucket.data;
        return 1;
    }
    return 0;
}
void volatileSetReset(volatileSetIterator *it) {
    raxStop(&it->bucket);
}
