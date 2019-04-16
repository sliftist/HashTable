#pragma once

#include "environment.h"

#include "RefCount.h"

#include "MemoryPool.h"

#ifdef __cplusplus
extern "C" {
#endif


#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // First, so we can overlap our .value with a uint64_t inside a union.
    uint64_t value;
    uint64_t version;
} AtomicUnit2;
#pragma pack(pop)

typedef struct {
    union {
        OutsideReference ref;
        AtomicUnit2 unit;
    };
} OutsideReferenceAtomic;

#pragma pack(push, 1)
typedef struct {
    // The refs for the memory sourceUnits and destUnits are in, moved here,
    //  to make it possible for hanging threads to still have valid source and dest memory.
    //  (the source and dest values don't really matter, they might as well be arbitrary refs)
    //  (destRef can be { 0 }, for example, when the dest and source are equal)
    //  (sourceRef can be  { 0 }, for example, when we are moving in static memory)
    // (these are destroyed whether or not we succeed in adding the transaction)
    OutsideReference sourceRef;
    OutsideReference destRef;

    MemoryPool* sourceRefMemPool;
    MemoryPool* destRefMemPool;

    // The reference we move
    //  (source can be nullptr, if we just want to wipe a value out.)
    //  (if dest is nullptr, it means we aren't moving a reference)
    OutsideReferenceAtomic* sourceRefToMove;
    OutsideReferenceAtomic* destRefToMove;

    uint64_t moveCount;
    AtomicUnit2* sourceUnits;
    uint64_t sourceConstValue;
    AtomicUnit2* destUnits;
} TransChange;
#pragma pack(pop)

#define MAX_CHANGE_COUNT 3

#pragma pack(push, 1)
typedef struct {
    uint64_t changeCount;
    TransChange changes[MAX_CHANGE_COUNT];

    // Populated and used internally
    uint64_t startVersion;
} Trans;
#pragma pack(pop)


// Should be initialized like:
//  TransApply value = TransApplyDefault();

#define TransApplyDefault() { { sizeof(Trans) + sizeof(InsideReference) } }

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    MemoryPool transPool;

    union {
        // Trans*
        OutsideReference transactionToApply;
        AtomicUnit2 transactionToApplyUnit;
    };

} TransApply;
#pragma pack(pop)


// Gets a version id, which if it still the current version id guarantees no more writes have occured.
uint64_t TransApply_GetVersion(TransApply* trans);

// Returns 0 if we ran it, 1 if we didn't because the version changed, and > 1 on errors.
//  If the version isn't equal to versionRead, we don't apply the transaction
int TransApply_Run(TransApply* trans, uint64_t versionRead, Trans transaction);



#ifdef __cplusplus
}
#endif