#pragma once

#include "MemPool.h"
#include "RefCount.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void*(*Allocate)(MemPool* pool, uint64_t size, uint64_t hash);
typedef void(*Free)(MemPool* pool, void* value);

#define MemPoolFixed_Allocations 8
#define MemPoolFixedDefault() { (Allocate)MemPoolFixed_Allocate, (Free)MemPoolFixed_Free }

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    uint64_t size: 63;
    uint64_t free: 1;
    void* allocation;
} MemPoolRecycle_Entry;
#pragma pack(pop)


#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    void* (*Allocate)(MemPool* pool, uint64_t size, uint64_t hash);
    void (*Free)(MemPool* pool, void* value);

    MemPoolRecycle_Entry allocations[MemPoolFixed_Allocations];
} MemPoolRecycle;
#pragma pack(pop)

void* MemPoolFixed_Allocate(MemPoolRecycle* pool, uint64_t size, uint64_t hash);
void MemPoolFixed_Free(MemPoolRecycle* pool, void* value);






// TODO: We only use 1 bit, so change the places that use this to only use 1 bit, instead of 1 byte

#pragma pack(push, 1)
typedef struct {
    union {
        struct {
            uint64_t totalAllocationsOutstanding: 63;
            uint64_t destructed: 1;
        };
        uint64_t valueForSet;
    };
} AllocCount;
#pragma pack(pop)

typedef struct MemPoolHashed MemPoolHashed;
// We only need 1 per table that has a hanging thread, so... as long as threads don't crash this just needs to be > the thread count.
//  An odd number here is important, so our iteration can increment by 2 and eventually reach every element
#define MAX_MEMPOOL_HISTORY 129
#pragma pack(push, 1)
typedef struct {
    uint64_t count;
    MemPoolHashed* pools[MAX_MEMPOOL_HISTORY];
} MemPools;
#pragma pack(pop)

#define MemPoolHashed_VALUE_OVERHEAD (1)
// VALUE_SIZE must already include MemPoolHashed_VALUE_OVERHEAD, (as you need to include it when making our allocation anyway...),
//  also either zero out our memory, or call MemPoolHashed_Initialize(pool).
// Only calls the freeCallback for referenced marked with Reference_Mark
#define MemPoolHashedDefault(VALUE_SIZE, VALUE_COUNT, VALUE_COUNT_LOG, \
callbackContext, freeCallback, getAllMemPools, releaseAllMemPools, hasEverAllocated, onNoMoreAllocations) { \
    (Allocate)MemPoolHashed_Allocate, (Free)MemPoolHashed_Free, \
    callbackContext, freeCallback, getAllMemPools, releaseAllMemPools, hasEverAllocated, onNoMoreAllocations, \
    (VALUE_SIZE - MemPoolHashed_VALUE_OVERHEAD), VALUE_COUNT, VALUE_COUNT_LOG, 0, 0 \
}

#pragma pack(push, 1)
typedef struct MemPoolHashed {
    void* (*Allocate)(MemPool* pool, uint64_t size, uint64_t hash);
    void (*Free)(MemPool* pool, void* value);

    void* callbackContext;

    void (*FreeCallback)(void* context, void* value);

    MemPools (*GetAllMemPools)(void* context);
    void (*ReleaseAllMemPools)(void* context, MemPools pools);

    bool (*HasEverAllocated)(MemPoolHashed* pool, uint64_t index);

    // Called back when we have no more allocations, and are destructed
    void (*OnNoMoreAllocations)(void* context);


    uint64_t VALUE_SIZE;
    uint64_t VALUE_COUNT;

    uint64_t VALUE_COUNT_LOG;

    AllocCount countForSet;

    // The memory after the end of the struct is...
    //  an array of values, of count VALUE_COUNT and size VALUE_SIZE
    //  size of (VALUE_COUNT + 7) / 8, parallel bits which indicate if values are in use
} MemPoolHashed;
#pragma pack(pop)

void MemPoolHashed_Initialize(MemPoolHashed* pool);

void* MemPoolHashed_Allocate(MemPoolHashed* pool, uint64_t size, uint64_t hash);
void MemPoolHashed_Free(MemPoolHashed* pool, void* value);
// When we have no more outstanding allocations, AND are destructed, we destroy holderOutsideReference.
//  (and that's all this does, it doesn't prevent allocations or anything, I don't think that is required...)
void MemPoolHashed_Destruct(MemPoolHashed* pool);
bool MemPoolHashed_IsInPool(MemPoolHashed* pool, void* address);

#ifdef __cplusplus
}
#endif