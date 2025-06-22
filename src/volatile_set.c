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

void freeRaxBuckets(void* p) {
    vsetBucket *bucket = p;
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
void freeVolatileSet(volatile_set *b) {
    if (!b) return;
    raxFreeWithCallback(b->expiry_buckets, freeRaxBuckets);
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

unsigned char *lpDeleteInteger(unsigned char *lp, long long target, int *deleted) {
    *deleted = 0;
    unsigned char *p = lpFirst(lp);
    while (p) {
        int64_t val;
        unsigned int slen;

        lpGetValue(p, &slen, &val);
        if (val == target) {
            lp = lpDelete(lp, p, NULL);
            *deleted = 1;
            break;
        }
        p = lpNext(lp, p);
    }
    return lp;
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
            uintptr_t pval = (uintptr_t) entry;
            int out;
            lp = lpDeleteInteger(lp, pval, &out);

            if (lpLength(lp) == 1) {
                // Downgrade back to SINGLE
                unsigned int slen;
                long long val;
                lpGetValue(lpFirst(lp), &slen, &val);
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

static void logField(robj *key, void *entry) {
    sds key2 = objectGetKey(key);
    serverLog(LL_WARNING, "key %s field %s value %s expired",
              key2, entryGetField(entry), entryGetValue(entry));
}

int volatileSetExpireEntry(volatile_set *set, void*serverDb, void*o, void *entry) {
    logField(  o,entry);
    if (set->etypr->expire) {
        set->etypr->expire(serverDb,o,entry);
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
    it->inner_it = NULL;
    it->state = 0;
}

int volatileSetNext(volatileSetIterator *it, void **entryptr) {
    while (1) {
        switch (it->state) {
            case 0: // Init or move to next bucket
                if (!raxNext(&it->bucket)) return 0;
                it->current_bucket = it->bucket.data;
                if (!it->current_bucket) continue;

                if (it->current_bucket->type == VSET_BUCKET_SINGLE) {
                    *entryptr = it->current_bucket->data.single;
                    it->state = 1;
                    return 1;
                } else if (it->current_bucket->type == VSET_BUCKET_LISTPACK) {
                    it->inner_it = NULL;
                    it->state = 2;
                    continue;
                } else if (it->current_bucket->type == VSET_BUCKET_HT) {
                    it->inner_it = hashtableCreateIterator(it->current_bucket->data.hashtable, HASHTABLE_ITER_SAFE);
                    it->state = 3;
                    continue;
                }
                break;

            case 1: // single already returned, move to next bucket
                it->state = 0;
                continue;

            case 2: { // listpack
                it->inner_it = it->inner_it
                                   ? lpNext(it->current_bucket->data.listpack, it->inner_it)
                                   : lpFirst(it->current_bucket->data.listpack);

                if (!it->inner_it) {
                    it->state = 0;
                    continue;
                }
                unsigned int slen;
                long long val;
                lpGetValue(it->inner_it, &slen, &val);
                *entryptr = (void *)(uintptr_t)val;
                return 1;
            }

            case 3: { // hashtable
                if (!hashtableNext(it->inner_it, entryptr)) {
                    hashtableReleaseIterator(it->inner_it);
                    it->inner_it = NULL;
                    it->state=0;
                    continue;
                }
                return 1;
            }
        }
    }
}

void volatileSetReset(volatileSetIterator *it) {
    raxStop(&it->bucket);
}

vsetBucket *volatileSetGetOldestBucketBelow(volatile_set *vs, uint64_t now) {
    if (!vs || !vs->expiry_buckets || vs->expiry_buckets->numele == 0) return NULL;

    unsigned char key[VSET_BUCKET_KEY_LEN];
    encodeExpiryBucketKey(&key, now);

    raxIterator iter;
    raxStart(&iter, vs->expiry_buckets);

    vsetBucket *result = NULL;

    // Find the last key <= now
    if (raxSeek(&iter, "<=", key, VSET_BUCKET_KEY_LEN)) {
        result = iter.data;
    }

    raxStop(&iter);
    return result;
}
