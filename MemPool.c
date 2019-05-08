#include "MemPool.h"
#include "MemPoolImpls.h"
#include "AtomicHelpers.h"
#include "environment.h"

MemPoolSystem memPoolSystem = MemPoolSystemDefault();

void* MemPoolSystem_Allocate(MemPoolSystem* pool, uint64_t size, uint64_t hash) {
	//uint64_t time = GetTime();
    void* allocation = malloc(size);
	//time = GetTime() - time;
	//printf("Alloc of %lluMB Took %llu million cycles\n", size / 1024 / 1024, time / 1024 / 1024);
	return allocation;
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
                return value.allocation;
            }
        }
    }

    void* allocation = malloc(size);
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


void MemPoolHashed_Initialize(MemPoolHashed* pool) {
    memset((byte*)pool + sizeof(MemPoolHashed), 0, (pool->VALUE_COUNT + 7) / 8);
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
    byte* allocatedBits = (byte*)pool + sizeof(MemPoolHashed);
    while(true) {
        uint32_t* pAllocatedByte = (uint32_t*)&allocatedBits[index / 8];
        uint32_t allocatedByte = *pAllocatedByte;
        uint32_t allocated = allocatedByte & (1 << (index % 8));
        if(!allocated) {
            if(InterlockedCompareExchange(
                (LONG32*)pAllocatedByte,
                allocatedByte | (1 << (index % 8)),
                allocatedByte
            ) == allocatedByte) {
                InterlockedIncrement64((LONG64*)&pool->totalAllocationsOutstanding);
                byte* allocation = allocatedBits + (pool->VALUE_COUNT + 7) / 8 + index * pool->VALUE_SIZE;
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
    uint64_t offset = (uint64_t)value - (uint64_t)((byte*)pool + sizeof(MemPoolHashed) + (pool->VALUE_COUNT + 7) / 8);
    if(offset % pool->VALUE_SIZE != 0) {
        // Value isn't aligned, this isn't allowed
        OnError(3);
    }
    uint64_t index = offset / pool->VALUE_SIZE;
    uint32_t* pAllocated = &((byte*)pool + sizeof(MemPoolHashed))[index / 8];
    byte isAllocated = *pAllocated & (1 << (index % 8));
    if(!isAllocated) {
        // Double free? Bad, we can't always catch this...
        OnError(5);
        return;
    }

    pool->FreeCallback(pool->freeCallbackContext, value);

    while(true) {
        uint32_t prevValue = *pAllocated;
        uint32_t newValue = prevValue & ~(1 << (index % 8));
        if(InterlockedCompareExchange(pAllocated, newValue, prevValue) == prevValue) {
            break;
        }
    }

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
    bool result = (uint64_t)pool <= (uint64_t)address && (uint64_t)address < (uint64_t)((byte*)pool + sizeof(MemPoolHashed) + (pool->VALUE_COUNT + 7) / 8 + pool->VALUE_SIZE * pool->VALUE_COUNT);
    #ifdef DEBUG
    if(result) {
        byte* valueStart = (byte*)pool + sizeof(MemPoolHashed) + (pool->VALUE_COUNT + 7) / 8;
        if(((uint64_t)address - (uint64_t)valueStart) % pool->VALUE_SIZE != 0) {
            // address is not aligned, so... it is not something we returned, it is a random address.
            //  Are you passing random addresses? Do you think you can use a random address in the pool? You clearly can't...
            OnError(5);
        }
    }
    #endif
    return result;
}