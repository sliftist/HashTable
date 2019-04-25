#pragma once

#include "environment.h"

#include "RefCount.h"

#include "MemPool.h"

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





#define MAX_CHANGE_COUNT 3




// Should be initialized like:
//  TransApply value = TransApplyDefault();

#define TransApplyDefault() { MemPoolFixedDefault() }

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    // TransChange*
    MemPoolFixed transPool;

    union {
        // TransChange*
        OutsideReference transactionToApply;
        AtomicUnit2 transactionToApplyUnit;
    };

    uint64_t curUniqueId;
    uint64_t padding;

} TransApply;
#pragma pack(pop)


// Gets the version, or returns 0 if TransApply_GetVersion must be called instead
uint64_t TransApply_FastGetVersion(TransApply* trans);

// Gets a version id, which if it still the current version id guarantees no more writes have occured.
uint64_t TransApply_GetVersion(TransApply* trans);

// Returns 0 if we ran it, 1 if we didn't because the version changed, and > 1 on errors.
//  If the version isn't equal to versionRead, we don't apply the transaction
int TransApply_Run(TransApply* trans, uint64_t* versionRead, TransChange transaction);



#ifdef __cplusplus
}
#endif