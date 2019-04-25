#pragma once

#include "environment.h"
#include "RefCount.h"

#define MEMORY_POOL_MAX_VALUES 3

#ifdef __cplusplus
extern "C" {
#endif

struct MemPool;
typedef struct MemPool MemPool;
#pragma pack(push, 1)
struct MemPool {
    // Pool must/does zero out memory it allocates
    void* (*Allocate)(MemPool* pool, uint64_t size, uint64_t hash);
    void (*Free)(MemPool* pool, void* value);
};
#pragma pack(pop)


#define MemPoolFixedDefault() { MemPoolFixed_Allocate, MemPoolFixed_Free }
#pragma pack(push, 1)
typedef struct {
    void* (*Allocate)(MemPoolFixed* pool, uint64_t size, uint64_t hash);
    void (*Free)(MemPoolFixed* pool, void* value);

    void* allocations[4];
} MemPoolFixed;
#pragma pack(pop)

void* MemPoolFixed_Allocate(MemPoolFixed* pool, uint64_t size, uint64_t hash);
void MemPoolFixed_Free(MemPoolFixed* pool, void* value);


#define MemPoolSystemDefault() { MemPoolSystem_Allocate, MemPoolSystem_Free }
typedef struct {
    void* (*Allocate)(MemPoolSystem* pool, uint64_t size, uint64_t hash);
    void (*Free)(MemPoolSystem* pool, void* value);
} MemPoolSystem;

void* MemPoolSystem_Allocate(MemPoolSystem* pool, uint64_t size, uint64_t hash);
void MemPoolSystem_Free(MemPoolSystem* pool, void* value);

MemPoolSystem memPoolSystem = MemPoolSystemDefault();


#define MemPoolHashed_VALUE_OVERHEAD() (sizeof(uint64_t))
#define MemPoolHashedDefault(VALUE_SIZE, VALUE_COUNT, holderOutsideReference, freeCallbackContext, freeCallback) { MemPoolHashed_Allocate, MemPoolHashed_Free, freeCallbackContext, freeCallback, VALUE_SIZE, VALUE_COUNT, holderOutsideReference, 0, 1 }
typedef struct {
    void* (*Allocate)(MemPoolHashed* pool, uint64_t size, uint64_t hash);
    void (*Free)(MemPoolHashed* pool, void* value);

    void* freeCallbackContext;
    void (*FreeCallback)(void* freeCallbackContext, void* value);

    uint64_t VALUE_SIZE;
    uint64_t VALUE_COUNT;
    OutsideReference holderOutsideReference;
    uint64_t totalAllocationsOutstanding;
    uint64_t destructed;
} MemPoolHashed;
void* MemPoolHashed_Allocate(MemPoolHashed* pool, uint64_t size, uint64_t hash);
void MemPoolHashed_Free(MemPoolHashed* pool, void* value);
// When we have no more outstanding allocations, AND are destructed, we destroy holderOutsideReference.
//  (and that's all this does, it doesn't prevent allocations or anything, I don't think that is required...)
void MemPoolHashed_Destruct(MemPoolHashed* pool);
void MemPoolHashed_IsInPool(MemPoolHashed* pool, void* address);


#ifdef __cplusplus
}
#endif