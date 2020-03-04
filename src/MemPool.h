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


extern uint64_t SystemAllocationCount;

#define MemPoolSystemDefault() { (Allocate)MemPoolSystem_Allocate, (Free)MemPoolSystem_Free }
#pragma pack(push, 1)
typedef struct {
	void* (*Allocate)(MemPool* pool, uint64_t size, uint64_t hash);
	void (*Free)(MemPool* pool, void* value);
} MemPoolSystem;
#pragma pack(pop)

void* MemPoolSystem_Allocate(MemPoolSystem* pool, uint64_t size, uint64_t hash);
void MemPoolSystem_Free(MemPoolSystem* pool, void* value);

extern MemPoolSystem memPoolSystem;


#ifdef __cplusplus
}
#endif