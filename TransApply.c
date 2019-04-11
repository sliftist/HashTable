#include "TransApply.h"
#include "AtomicHelpers.h"

#pragma pack(push, 1)
typedef struct {
    AtomicUnit2* unit;

    uint64_t newValue;

    // This is exclusively used to noop hanging writes (when two threads try to complete the same operation),
    //  and doesn't provide any other transactional security.
    uint64_t originalVersion;
} TransPart;
#pragma pack(pop)

typedef struct {
    uint64_t allocationSize;

    uint64_t refCount;
    OutsideReference refs[MAX_REFS_PER_TRANSACTION];

    uint64_t partCount;
    uint64_t partCountUsed;
    TransPart* parts;

} TransAllocation;

// True on success
bool atomicWrite(AtomicUnit2* unit, uint64_t newValue, uint64_t originalVersion) {
    AtomicUnit2 current = *unit;
    if(current.version != originalVersion) {
        return false;
    }
    AtomicUnit2 newUnit = current;
    newUnit.value = newValue;
    newUnit.version = originalVersion + 1;

    return InterlockedCompareExchangeStruct128(unit, &current, &newUnit);
}

void freedTransaction(TransApply* trans, InsideReference* transRef) {
    if(!transRef) return;
    if(trans->recycledTrans) return;
    if(InterlockedCompareExchange(&trans->recycledTrans, transRef, 0) != 0) {
        // If there is already another recycled transaction, leave it there. If it isn't sufficient in size, it will
        //  be exclusively taken and freed, so this never falls into a hole of never using recycledTrans.
        free(transRef);
    }
}


void transaction_applyWrites(TransApply* trans, TransAllocation* transaction) {
    // transactions won't be reused until all references are gone, so refCount and partCount can be considered constant.
    uint64_t refCount = transaction->refCount;
    uint64_t partCountUsed = transaction->partCountUsed;

    uint64_t refsTaken = 0;
    InsideReference refs[MAX_REFS_PER_TRANSACTION] = { 0 };

    for(uint64_t i = 0; i < refCount; i++) {
        // If any memory has been freed, then someone else must have finished the transaction. So... release our references, and return
        if(!Reference_Acquire(&transaction->refs[i])) {
            break;
        }
        refsTaken = i + 1;
    }

    if(refsTaken == refCount) {        
        for(uint64_t i = 0; i < partCountUsed; i++) {
            // This can also be considered to be immutable
            TransPart part = transaction->parts[i];
            if(!atomicWrite(part.unit, part.newValue, part.originalVersion)) {
                // We don't need to break... but we might as well...
                break;
            }
        }
    }

    for(uint64_t i = 0; i < refsTaken; i++) {
        InsideReference* freedTrans = nullptr;
        Reference_Release(&transaction->refs[i], &refs[i], &freedTrans);
        freedTransaction(trans, freedTrans);
    }
}

uint64_t Transaction_getVersion(TransApply* trans) {
    while(true) {
        uint64_t version = trans->transactionToApplyUnit.version;
        InsideReference* transRef = Reference_Acquire(&trans->transactionToApply);
        if(!transRef) {
            // It may be the case that version is for an old write, and we read that there is no transaction for a newer write.
            //  This is fine, it just means (later) when the version is compared to see if anything more was written we will see more
            //  has been written, and run this code again.
            return version;
        }
        // It may be that version does not correspond with transactionToApply. That is fine too, we won't be returning this version,
        //  we just had to read it before we read transactionToApply.

        transaction_applyWrites(trans, (TransAllocation*)PACKED_POINTER_GET_POINTER(*transRef));

        InsideReference* freedTrans = nullptr;
        Reference_DestroyOutside(&trans->transactionToApply, transRef, &freedTrans);
        freedTransaction(trans, freedTrans);
    }
}




typedef struct {
    uint64_t nextPartIndex;
    TransAllocation* transAllocation;
} WriteToUnitContext;
void transaction_writeToUnit(WriteToUnitContext* context, AtomicUnit2* unit, uint64_t value) {
    if(context->nextPartIndex >= context->transAllocation->partCount) {
        // Added more writes than the specified maximum writes...
        OnError(3);
        return;
    }

    TransPart newPart = { 0 };
    newPart.unit = unit;
    newPart.newValue = value;
    newPart.originalVersion = unit->version;

    context->transAllocation->parts[context->nextPartIndex++] = newPart;
}

// Returns true if we ran it, false if we didn't because the version changed
int Transaction_Run(
    TransApply* trans,

    // If the version isn't equal to this, we don't apply the transaction
    //  (this should be the version read before getting the passed in outside refs)
    uint64_t versionRead,

    // Upon success we take ownership of these references, otherwise... you should free them
    uint64_t refCount,
    // (can be stack allocated, we copy these locally)
    InsideReference* refs,

    // The more consistent this is the better
    uint64_t transactionMaxWrites,

    void* runTransactionContext,
    // Writes aren't apply until after we finish running this function, so... if you read to something
    //  you wrote to, you will get the original value.
    void (*runTransaction)(
        void* runTransactionContext,
        void* writeToUnitContext,
        void (*writeToUnit)(void* writeToUnitContext, AtomicUnit2* unit, uint64_t value)
    )
) {
    if(refCount > MAX_REFS_PER_TRANSACTION) {
        // refCount is too high
        OnError(3);
        return 3;
    }
    // We only technically need to check this at the end, and we don't want to check it too often as the overwhelming
    //  amount of times it should be equal. But... once at the beginning is probably good...
    if(versionRead != trans->transactionToApply.value) {
        return 1;
    }

    OutsideReference transAllocationOutsideRef = { 0 };
    TransAllocation* transAllocation = nullptr;

    uint64_t allocationSize = transactionMaxWrites * sizeof(TransPart) + sizeof(TransAllocation);
    
    if(trans->recycledTrans) {
        InsideReference* recycled = (void*)InterlockedCompareExchange64((LONG64)&trans->recycledTrans, nullptr, (LONG64)trans->recycledTrans);
        if(recycled) {
            transAllocation = (void*)PACKED_POINTER_GET_POINTER(*recycled);

            if(transAllocation->allocationSize < allocationSize) {
                free(recycled);
                transAllocation = nullptr;
            } else {
                if(!Reference_SetOutside(&transAllocationOutsideRef, recycled, nullptr)) {
                    // Impossible... transAllocationOutsideRef is local to our thread...
                    OnError(9);
                    return 9;
                }
            }
        }
    }

    if(transAllocation) {
        memset(transAllocation, 0, transAllocation->allocationSize);
    } else {
        Reference_Allocate(
            allocationSize,
            &transAllocationOutsideRef,
            &transAllocation
        );
        if(!transAllocation) {
            OutsideReference forceInsideRelease = { 0 };
            // Allocation failed, this is bad, just abort, but pretend like we succeeded, because retries will only
            //  cause more system instability.
            for(uint64_t i = 0; i < refCount; i++) {
                Reference_Release(&forceInsideRelease, &refs[i], nullptr);
            }
            return 2;
        }
        memset(transAllocation, 0, allocationSize);
        transAllocation->allocationSize = allocationSize;
    }

    transAllocation->partCountUsed = 0;
    transAllocation->parts = (void*)((byte*)transAllocation + sizeof(TransAllocation));
    transAllocation->partCount = transactionMaxWrites;
    transAllocation->refCount = refCount;
    

    {
        uint64_t nextPartIndex = 0;

        WriteToUnitContext writeContext = { 0 };
        writeContext.nextPartIndex = 0;
        writeContext.transAllocation = transAllocation;

        runTransaction(runTransactionContext, &writeContext, transaction_writeToUnit);

        transAllocation->partCountUsed = writeContext.nextPartIndex;
    }


    OutsideReference forceInsideRelease = { 0 };
    for(uint64_t i = 0; i < refCount; i++) {
        // I am pretty sure the caller could pass in outside references, as outside references stored in thread
        //  local storage (on the stack) guarantee at least one reference stays alive. But... forcing the caller to
        //  pass inside references guarantees they acquired a reference... or something... I'm not sure.
        Reference_SetOutside(&transAllocation->refs[i], &refs[i], nullptr);
        Reference_Release(&forceInsideRelease, &refs[i], nullptr);
    }
   

    // (because we called transaction_applyAllWrite earlier it means if this works we always set the transaction
    //  state when there is no value, which means we don't have to free anything on success here)
    if(!atomicWrite(&trans->transactionToApplyUnit, transAllocationOutsideRef.value, versionRead)) {
        return 1;
    }

    // Call this to apply the transaction we just pushed.
    Transaction_getVersion(trans);
    return 0;
}