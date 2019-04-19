#pragma once

#include "environment.h"

#define MEMORY_POOL_MAX_VALUES 3

#ifdef __cplusplus
extern "C" {
#endif

// TODO: Defined this via a macro, so we can stack allocate a few objects.

// MemoryPool pool = { SIZE };
#pragma pack(push, 1)
typedef struct {
    uint64_t SIZE;

    void* values[MEMORY_POOL_MAX_VALUES];
} MemoryPool;
#pragma pack(pop)

void* MemoryPool_Allocate(MemoryPool* self);
void MemoryPool_Free(MemoryPool* self, void* value);

#ifdef __cplusplus
}
#endif