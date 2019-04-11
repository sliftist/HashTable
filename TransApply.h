#pragma once

#include "environment.h"

#include "RefCount.h"

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



#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
    union {
        OutsideReference transactionToApply;
        AtomicUnit2 transactionToApplyUnit;
    };

    // Has no references to it, is returned as should be freed by RefCount
    InsideReference* recycledTrans;
} TransApply;
#pragma pack(pop)

// Gets a version id, which if it still the current version id guarantees no more writes have occured.
uint64_t Transaction_getVersion(
    TransApply* trans
);

#define MAX_REFS_PER_TRANSACTION 10

// Returns 0 if we ran it, 1 if we didn't because the version changed, and > 1 on errors.
int Transaction_Run(
    TransApply* trans,

    // If the version isn't equal to this, we don't apply the transaction
    uint64_t versionApplied,

    // Upon success we take ownership of these references
    uint64_t refCount,
    OutsideReference* refs,

    // The more consistent this number is the better. Ideally it would be constant for any given TransApply...
    uint64_t transactionMaxWrites,

    void* runTransactionContext,
    // Writes aren't apply until after we finish running this function, so... if you read to something
    //  you wrote to, you will get the original value.
    void (*runTransaction)(
        void* runTransactionContext,
        void* writeToUnitContext,
        void (*writeToUnit)(void* writeToUnitContext, AtomicUnit2* unit, uint64_t value)
    )
);



#ifdef __cplusplus
}
#endif