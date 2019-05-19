#pragma once

#include "environment.h"


#ifdef __cplusplus
extern "C" {
#endif

/*

An allocator built for tests.

Doesn't actually release memory until BulkAlloc_dtor is called.

BulkAlloc_dtor verifies all BulkAlloc_alloc calls have a corresponding BulkAlloc_free call.

BulkAlloc_free does some loose checks for double frees.

Works best if most old memory is freed before new memory (which tests likely do).

Supports multi-threading (of course) (but ctor and dtor aren't).

~128GB (allocation count) limit (can be increased in size relatively easily though), which includes our overhead per allocation (4 bytes).

Becomes O(N) where N is the amount of previous allocations when allocations become highly fragmented (but still dense).
    So, it is required that most allocations outlive allocations allocated before them to maintain O(1) complexity.

Oh, and every alloc must be the same size...

*/

#pragma pack(push, 1)
typedef struct {
    uint32_t allocated;
} BulkAlloc_AllocHead;
#pragma pack(pop)


#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    uint32_t count;
    uint32_t countUsed;
    byte* allocation;
} BulkAlloc_Alloc;
#pragma pack(pop)
CASSERT(sizeof(BulkAlloc_Alloc) == 16);

#pragma pack(push, 1)
typedef struct {
    BulkAlloc_Alloc data;
    uint64_t searchIndex;
} BulkAlloc_AllocFull;
#pragma pack(pop)

#define BulkAlloc_Default() {0}

#define BulkAlloc_AllocationCount 64

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // Allocations grow in size until they hit 2^32, at which point they level off. This is required because BulkAlloc_Alloc has to be
    //  128 bits. However... it is fine, because we have enough entries that this should provide 32*4GB, so... more than enough.
    BulkAlloc_AllocFull allocations[BulkAlloc_AllocationCount];
    uint64_t allocSearchIndex;
    uint64_t size;
    bool initialized;
} BulkAlloc;
#pragma pack(pop)

void BulkAlloc_ctor(BulkAlloc* self, uint64_t allocSize);
void* BulkAlloc_alloc(BulkAlloc* self);
void BulkAlloc_free(BulkAlloc* self, void* pointer);
void BulkAlloc_dtor(BulkAlloc* self);
void BulkAlloc_dtorIgnoreLeaks(BulkAlloc* self);

bool BulkAlloc_isAllocated(BulkAlloc* self, void* pointer);



#ifdef __cplusplus
}
#endif