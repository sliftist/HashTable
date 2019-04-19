#include "TransApply.h"
#include "AtomicHelpers.h"

//todonext
// Finish rewriting this to reflect the greatly simplified runTransaction code AND
//  to make the caller pass an object with all the transaction changes at once
//  (perhaps via an InsideReference*? Which we then make an OutsideReference from).
// And then make a memory pool object to reuse memory (make it pretty simple,
//  probably just with 3 slots, and it will require every allocation to be the same
//  size).
// Then... AtomicHashTable2 needs a helper function to read a whole AtomicSlot, and
//  retry if anything changed, eventually using the version to insert a remove or insert
//  (having previous dealt with moves).
//  - Hmm... and I think, if a remove gets interrupted, that moving can fix up the table anyway,
//      filling in any squares, and deleting skip values.
//  OH! And we will probably have two sizes for transactions, which makes it even better to
//  have the caller manage the memory recycling!


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

// Returns true if we see a more recent version
bool transaction_applyWrite(
    TransApply* trans,
    Trans* transaction,
    TransChange* change,
    InsideReference* sourceRef,
    InsideReference* destRef
) {
    if(change->destUnits) {
        for(uint64_t i = 0; i < change->moveCount; i++) {
            AtomicUnit2 newUnit = { 0 };
            if(change->sourceUnits) {
				//todonext
				// So... the microsoft compiler has a bug, where it uses xmm, BUT sourceUnits isn't 128 bit aligned. However... that's fine, sourceUnits should be 128 bit aligned,
				//	we should verify it is much earlier on, because it can be, and if it is it will make our copying code faster.
                newUnit = change->sourceUnits[i];
            } else {
                newUnit.value = change->sourceConstValue;
            }
            newUnit.version = transaction->startVersion;

			AtomicUnit2* pCurrent = &change->destUnits[i];
			AtomicUnit2 current = *pCurrent;
            if(current.version > newUnit.version) {
                return true;
            }
            // We have to continue, as another thread may have only gotten part of the way through applying the transaction,
            //  and then crashed, or been paused.
            if(current.version == newUnit.version) {
                continue;
            }

			// TODO: We can actually retry here if the change isn't a version change. This will let this code be a bit faster.
			if (!InterlockedCompareExchangeStruct128(
				pCurrent,
				&current,
				&newUnit
			)) {
				return true;
			}
        }
    }

    // Move the reference last
    if(change->destRefToMove) {
        // First we duplicate the source, making a second OutsideReference, then we atomically put that in the destination,
        //  swapping them, and checking the version with a 128 bit CAS. Then we destroy the original source reference,
        //  and then we destroy the original destination reference.
        OutsideReferenceAtomic currentRefToMove = *change->destRefToMove;
        if(currentRefToMove.unit.version < transaction->startVersion) {
            // We only want to move the reference once, as it is more expensive to try and move it

            OutsideReferenceAtomic newRefToMove = { 0 };
            InsideReference* sourceRefToMove = nullptr;
            if(change->sourceRefToMove) {
                sourceRefToMove = Reference_Acquire(&change->sourceRefToMove->ref);
                if(!sourceRefToMove) {
                    // Value has been deleted (but a transaction could move and then delete something, so we have to still continue,
                    //  it is just that this change is done)
                    return false;
                }
                Reference_SetOutside(&newRefToMove.ref, sourceRefToMove);
            }
            newRefToMove.unit.version = transaction->startVersion;

            if(!InterlockedCompareExchangeStruct128(change->destRefToMove, &currentRefToMove, &newRefToMove)) {
                if(sourceRefToMove) {
                    Reference_DestroyOutside(&newRefToMove.ref, sourceRefToMove);
                }
            } else {
                // else the outside reference is now in the dest, and so is alive, in shared memory
                //  And... the previous value has been atomically destroyed, so moved to us, so we can/have to destroy it...

                InsideReference* prevRef = Reference_Acquire(&currentRefToMove.ref);
                if(prevRef) {
                    Reference_DestroyOutside(&currentRefToMove.ref, prevRef);
                    Reference_Release(nullptr, prevRef, false);
                }
            }
            
            if(sourceRefToMove) {
                // We can't release from newRefToMove, as it has been forcefully moved to new memory, and so
                //  its count no longer means anything.
                Reference_Release(nullptr, sourceRefToMove, false);
            }
        }
    }

    return false;
}

void transaction_applyWrites(TransApply* trans, Trans* transaction) {
    for(uint64_t t = 0; t < transaction->changeCount; t++) {
        TransChange* change = &transaction->changes[t];

        InsideReference* sourceRef = nullptr;
        if(change->sourceRef.pointerClipped) {
            sourceRef = Reference_Acquire(&change->sourceRef);
            if(!sourceRef) continue;
        }
        InsideReference* destRef = nullptr;
        if(change->destRef.pointerClipped) {
            destRef = Reference_Acquire(&change->destRef);

            if(!destRef) {
                if(Reference_Release(&change->sourceRef, sourceRef, true)) {
                    MemoryPool_Free(change->sourceRefMemPool, sourceRef);
                }
                continue;
            }
        }

        bool newVersionSeen = transaction_applyWrite(
            trans,
            transaction,
            change,
            sourceRef,
            destRef
        );

        if(sourceRef) {
            if(Reference_Release(&change->sourceRef, sourceRef, true)) {
                MemoryPool_Free(change->sourceRefMemPool, sourceRef);
            }
            Reference_DestroyOutside(&change->sourceRef, sourceRef);
        }
        if(destRef) {
            if(Reference_Release(&change->destRef, destRef, true)) {
                MemoryPool_Free(change->destRefMemPool, destRef);
            }
            Reference_DestroyOutside(&change->destRef, destRef);
        }

        if(newVersionSeen) {
            return;
        }
    }
}

void transApply_freeTransaction(
    TransApply* trans,
    InsideReference* transRef,
    Trans* transaction
) {
    for(uint64_t i = 0; i < transaction->changeCount; i++) {
        // We could unsafely get the inner reference, because we should have exclusive control over the transaction,
        //  and therefore exclusive control over that outside reference memory. But... there's no need
        TransChange* change = &transaction->changes[i];
        
		if (change->sourceRef.pointerClipped) {
			InsideReference* sourceRef = Reference_Acquire(&change->sourceRef);
			Reference_DestroyOutside(&change->sourceRef, sourceRef);
			if (Reference_Release(&change->sourceRef, sourceRef, true)) {
				MemoryPool_Free(change->sourceRefMemPool, sourceRef);
			}
		}
        
		if (change->destRef.pointerClipped) {
			InsideReference* destRef = Reference_Acquire(&change->destRef);
			Reference_DestroyOutside(&change->destRef, destRef);
			if (Reference_Release(&change->destRef, destRef, true)) {
				MemoryPool_Free(change->destRefMemPool, destRef);
			}
		}
    }
    MemoryPool_Free(&trans->transPool, transRef);
}

uint64_t TransApply_FastGetVersion(TransApply* trans) {
    uint64_t version = trans->transactionToApplyUnit.version;
    if(!trans->transactionToApply.pointerClipped) {
        return version;
    }
    return 0;
}

uint64_t TransApply_GetVersion(TransApply* trans) {
    while(true) {
        // Doesn't need a memory fence.
        //  - If we read the same version before and after, and both times there is nothing to write,
        //      then even if another thread did write, we can't have read the writes, or else we would have
        //      read out of order (it must have written to version first), which doesn't happen.
        //  (https://xem.github.io/minix86/manual/intel-x86-and-64-manual-vol3/o_fe12b1e2a880e0ce-262.html)

        uint64_t version = trans->transactionToApplyUnit.version;
        if(!trans->transactionToApply.pointerClipped) {
            return version;
        }
        InsideReference* transRef = Reference_Acquire(&trans->transactionToApply);
        if(!transRef) {
            // It may be the case that version is for an old write, and we read that there is no transaction for a newer write.
            //  This is fine, it just means (later) when the version is compared to see if anything more was written we will see more
            //  has been written, and run this code again.
            return version;
        }
        // It may be that version does not correspond with transactionToApply. That is fine too, we won't be returning this version,
        //  we just had to read it before we read transactionToApply.

        Trans* transaction = transRef->pointer;
        transaction_applyWrites(trans, transaction);

        
        Reference_DestroyOutside(&trans->transactionToApply, transRef);

        if(Reference_Release(&trans->transactionToApply, transRef, true)) {
            transApply_freeTransaction(trans, transRef, transaction);
        }
    }
}



// Returns 0 if we ran it, 1 if we didn't because the version changed, and > 1 on errors.
int TransApply_Run(
    TransApply* trans,
    // If the version isn't equal to this, we don't apply the transaction
    uint64_t versionRead,
    Trans transaction
) {
    // We only technically need to check this at the end, and we don't want to check it too often as the overwhelming
    //  amount of times it should be equal. But... once at the beginning is probably good...
    if(versionRead != trans->transactionToApplyUnit.version) {
        return 1;
    }

    if(transaction.changeCount > MAX_CHANGE_COUNT) {
        // Too many changes
        OnError(5);
        return 0;
    }
    for(uint64_t i = 0; i < transaction.changeCount; i++) {
        TransChange change = transaction.changes[i];
        if((uint64_t)change.sourceUnits % 16 != 0) {
            // sourceUnits should be 16 byte aligned, as this lets us make the copy code much
            //  faster (also MSVC uses movaps incorrectly, a 16 byte aligned move, so if we don't
            //  check this, we may crash later).
            OnError(6);
        }
        if((uint64_t)change.destUnits % 16 != 0) {
            // destUnits must be 16 byte aligned
            OnError(6);
        }
    }

    // We have to move the transaction off the stack, into the heap (or whatever), so that hanging threaded operations
    //  using the transaction don't crash!

    void* allocation = MemoryPool_Allocate(&trans->transPool);

    if(!allocation) {
        return 2;
    }

    OutsideReference transAllocationOutsideRef = { 0 };
    Trans* transAllocation = nullptr;
    Reference_RecycleAllocate(allocation, sizeof(Trans), &transAllocationOutsideRef, &transAllocation);

	transaction.startVersion = InterlockedIncrement64((LONG64*)&trans->curUniqueId);
    *transAllocation = transaction;


    // (because we called transaction_applyAllWrite earlier it means if this works we always set the transaction
    //  state when there is no value, which means we don't have to free anything on success here)
    if(!atomicWrite(&trans->transactionToApplyUnit, transAllocationOutsideRef.value, versionRead)) {
        transApply_freeTransaction(trans, allocation, transAllocation);
        return 1;
    }

    // Call this to apply the transaction we just pushed.
    TransApply_GetVersion(trans);
    return 0;
}