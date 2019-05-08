#pragma once

#include "environment.h"

#ifdef __cplusplus
extern "C" {
#endif

struct MemPool;
typedef struct MemPool MemPool;
#pragma pack(push, 1)
struct MemPool {
    // Pool does not initialize memory
    void* (*Allocate)(MemPool* pool, uint64_t size, uint64_t hash);
    void (*Free)(MemPool* pool, void* value);
};
#pragma pack(pop)



#ifdef __cplusplus
}
#endif