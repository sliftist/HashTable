#pragma once

#include "MemPool.h"
#include "RefCount.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MemPoolFixed_Allocations 8
#define MemPoolFixedDefault() { MemPoolFixed_Allocate, MemPoolFixed_Free }

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



#define MemPoolSystemDefault() { MemPoolSystem_Allocate, MemPoolSystem_Free }
#pragma pack(push, 1)
typedef struct {
    void* (*Allocate)(MemPool* pool, uint64_t size, uint64_t hash);
    void (*Free)(MemPool* pool, void* value);
} MemPoolSystem;
#pragma pack(pop)

void* MemPoolSystem_Allocate(MemPoolSystem* pool, uint64_t size, uint64_t hash);
void MemPoolSystem_Free(MemPoolSystem* pool, void* value);

MemPoolSystem memPoolSystem = MemPoolSystemDefault();

#pragma pack(push, 1)
typedef struct {
    uint64_t allocated;
} MemPoolHashed_InternalEntry;
#pragma pack(pop)

#define MemPoolHashed_VALUE_OVERHEAD (sizeof(MemPoolHashed_InternalEntry))
// VALUE_SIZE must already include MemPoolHashed_VALUE_OVERHEAD, (as you need to include it when making our allocation anyway...)
#define MemPoolHashedDefault(VALUE_SIZE, VALUE_COUNT, VALUE_COUNT_LOG, holderOutsideReference, freeCallbackContext, freeCallback) { MemPoolHashed_Allocate, MemPoolHashed_Free, freeCallbackContext, freeCallback, VALUE_SIZE, VALUE_COUNT, VALUE_COUNT_LOG, holderOutsideReference, 0, 1 }
#pragma pack(push, 1)
typedef struct {
    void* (*Allocate)(MemPool* pool, uint64_t size, uint64_t hash);
    void (*Free)(MemPool* pool, void* value);

    void* freeCallbackContext;
    // We call this with the avlue passed to our free, before we actually free the memory (or do whatever we do with it).
    void (*FreeCallback)(void* freeCallbackContext, void* value);

    uint64_t VALUE_SIZE;
    uint64_t VALUE_COUNT;

    uint64_t VALUE_COUNT_LOG;
    OutsideReference holderOutsideReference;

    uint64_t totalAllocationsOutstanding;
    uint64_t destructed;

    // The memory after the end of the struct is an array of values, of count VALUE_COUNT and size VALUE_SIZE
} MemPoolHashed;
#pragma pack(pop)
void* MemPoolHashed_Allocate(MemPoolHashed* pool, uint64_t size, uint64_t hash);
void MemPoolHashed_Free(MemPoolHashed* pool, void* value);
// When we have no more outstanding allocations, AND are destructed, we destroy holderOutsideReference.
//  (and that's all this does, it doesn't prevent allocations or anything, I don't think that is required...)
void MemPoolHashed_Destruct(MemPoolHashed* pool);
bool MemPoolHashed_IsInPool(MemPoolHashed* pool, void* address);

#ifdef __cplusplus
}
#endif