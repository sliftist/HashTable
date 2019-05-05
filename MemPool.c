#include "MemPool.h"
#include "MemPoolImpls.h"
#include "AtomicHelpers.h"
#include "environment.h"

MemPoolSystem memPoolSystem = MemPoolSystemDefault();

void* zeroAlloc(uint64_t size) {
    byte* memory = malloc(size);
    if(memory != 0) {
        memset(memory, 0, size);
    }
    return memory;
}

void* MemPoolSystem_Allocate(MemPoolSystem* pool, uint64_t size, uint64_t hash) {
    return zeroAlloc(size);
}
void MemPoolSystem_Free(MemPoolSystem* pool, void* value) {
    if(!value) return;
    free(value);
}



void* MemPoolFixed_Allocate(MemPoolRecycle* this, uint64_t size, uint64_t hash) {
    // Try reusing an existing free entry
    for(uint64_t i = 0; i < MemPoolFixed_Allocations; i++) {
        MemPoolRecycle_Entry value = this->allocations[i];
        if(value.allocation && value.free && value.size >= size) {
            MemPoolRecycle_Entry newValue = value;
            newValue.free = 0;
            if(InterlockedCompareExchangeStruct128(
                &this->allocations[i],
                &value,
                &newValue
            )) {
                memset(value.allocation, 0, value.size);
                return value.allocation;
            }
        }
    }

    void* allocation = zeroAlloc(size);
	if (!allocation) {
		return nullptr;
	}

    // Try displacing a smaller freed entry to store it
    for(uint64_t i = 0; i < MemPoolFixed_Allocations; i++) {
        MemPoolRecycle_Entry value = this->allocations[i];
        if(!value.allocation || value.free && value.size < size) {
            MemPoolRecycle_Entry newValue;
            newValue.allocation = allocation;
            newValue.size = size;
            newValue.free = 0;
            if(InterlockedCompareExchangeStruct128(
                &this->allocations[i],
                &value,
                &newValue
            )) {
                // If we removed a value to put a larger allocation, free the previous allocation
                if(value.allocation) {
                    free(value.allocation);
                }
                return allocation;
            }
        }
    }

    // Try displacing a smaller allocated entry to store it
    for(uint64_t i = 0; i < MemPoolFixed_Allocations; i++) {
        MemPoolRecycle_Entry value = this->allocations[i];
        if(value.allocation && !value.free && value.size < size) {
            MemPoolRecycle_Entry newValue;
            newValue.allocation = allocation;
            newValue.size = size;
            newValue.free = 0;
            if(InterlockedCompareExchangeStruct128(
                &this->allocations[i],
                &value,
                &newValue
            )) {
                return allocation;
            }
        }
    }

    return allocation;
}
void MemPoolFixed_Free(MemPoolRecycle* this, void* value) {
    if(!value) return;
    for(uint64_t i = 0; i < MemPoolFixed_Allocations; i++) {
        MemPoolRecycle_Entry entry = this->allocations[i];
        if(entry.allocation == value) {
            if(entry.free) {
                // Attempted double free of value, this is bad, as we just got lucky to catch this,
                //  and wouldn't if the allocation got moved out of allocations due to too many threads,
                //  or just too many concurrent allocations.
                OnError(4);
                return;
            }
            MemPoolRecycle_Entry newValue = entry;
            newValue.free = 1;
            if(InterlockedCompareExchangeStruct128(
                &this->allocations[i],
                &entry,
                &newValue
            )) {
                return;
            }
            // Else means we decided not to reuse it and evacuated it/
            break;
        }
    }

    // We must have decided we weren't going to reuse even before we free it, because the
    //  allocation was just too small...
    free(value);
}



void memPoolHashed_DestroyOutsideRef(MemPoolHashed* pool) {
    InsideReference* ref = Reference_Acquire(&pool->holderOutsideReference);
    if(!ref) {
        // We should uniquely hold this...
        OnError(2);
        return;
    }
    Reference_DestroyOutside(&pool->holderOutsideReference, ref);
    Reference_Release(&emptyReference, ref);
}

void* MemPoolHashed_Allocate(MemPoolHashed* pool, uint64_t size, uint64_t hash) {
    if(size != pool->VALUE_SIZE - MemPoolHashed_VALUE_OVERHEAD) {
        // Invalid input, size must equal the size the pool was initialized with (- MemPoolHashed_VALUE_OVERHEAD, which we use)
        OnError(2);
        return nullptr;
    }
	// Tried to allocate after destruct was called...
	if (pool->destructed) {
		// This is a valid necessary race condition, so we just have to return nullptr.
		return nullptr;
	}
    uint64_t index = hash >> (64 - pool->VALUE_COUNT_LOG);
    uint64_t loopCount = 0;
    byte* valueStart = (byte*)pool + sizeof(MemPoolHashed);
    while(true) {
        MemPoolHashed_InternalEntry* entry = (void*)(valueStart + index * pool->VALUE_SIZE);
        if(!entry->allocated) {
            if(InterlockedCompareExchange64(
                (LONG64*)&entry->allocated,
                1,
                0
            ) == 0) {
                InterlockedIncrement64((LONG64*)&pool->totalAllocationsOutstanding);
                byte* allocation = (byte*)entry + sizeof(MemPoolHashed_InternalEntry);
                memset(allocation, 0, size);
                return allocation;
            }
        }
        index = (index + 1) % pool->VALUE_COUNT;
        if(index == 0) {
            loopCount++;
            if(loopCount >= 10) {
                // The pool is full... this shouldn't be possible...
                OnError(9);
                return nullptr;
            }
        }
    }
    // Unreachable...
    OnError(13);
    return nullptr;
}
void MemPoolHashed_Free(MemPoolHashed* pool, void* value) {
    if(!value) return;
    // So... interestingly enough, because our pool is continous, we CAN TELL where the value is, just by the pointer. Cool...
    MemPoolHashed_InternalEntry* entry = (void*)((byte*)value - sizeof(MemPoolHashed_InternalEntry));
    if(!entry->allocated) {
        // Double free? Bad, we can't always catch this...
        OnError(5);
        return;
    }

    pool->FreeCallback(pool->freeCallbackContext, value);

    // Eh... we might run out of memory because we don't have a memory barrier here... which means our
    //  frees may take a while to be visible, even when other interlocked operations make it inferrable
    //  to other threads that there should be enough entries free... But... it should be fine...
    //  - And actually... our reserved count can't propogate before our allocated changed, can it?
    entry->allocated = 0;

    uint64_t outstandingAfter = InterlockedDecrement64((LONG64*)&pool->totalAllocationsOutstanding);
    if(outstandingAfter == 0 && pool->destructed) {
        memPoolHashed_DestroyOutsideRef(pool);
    }
}
// When we have no more outstanding allocations, AND are destructed, we destroy holderOutsideReference.
//  (and that's all this does, it doesn't prevent allocations or anything, I don't think that is required...)
void MemPoolHashed_Destruct(MemPoolHashed* pool) {
    if(InterlockedCompareExchange64(
        (LONG64*)&pool->destructed,
        1,
        0
    ) != 0) {
        // Destruct may be called multiple times
        return;
    }
    if(pool->totalAllocationsOutstanding == 0) {
        memPoolHashed_DestroyOutsideRef(pool);
    }
}
bool MemPoolHashed_IsInPool(MemPoolHashed* pool, void* address) {
    bool result = (uint64_t)pool <= (uint64_t)address && (uint64_t)address < (uint64_t)((byte*)pool + sizeof(MemPoolHashed) + pool->VALUE_SIZE * pool->VALUE_COUNT);
    #ifdef DEBUG
    if(result) {
        byte* valueStart = (byte*)pool + sizeof(MemPoolHashed);
        if(((uint64_t)address - (uint64_t)valueStart - MemPoolHashed_VALUE_OVERHEAD) % pool->VALUE_SIZE != 0) {
            // address is not aligned, so... it is not something we returned, it is a random address.
            //  Are you passing random addresses? Do you think you can use a random address in the pool? You clearly can't...
            OnError(5);
        }
    }
    #endif
    return result;
}