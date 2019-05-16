#include "MemPool.h"
#include "MemPoolImpls.h"
#include "AtomicHelpers.h"
#include "environment.h"

MemPoolSystem memPoolSystem = MemPoolSystemDefault();

uint64_t SystemAllocationCount = 0;

void* MemPoolSystem_Allocate(MemPoolSystem* pool, uint64_t size, uint64_t hash) {
	//uint64_t time = GetTime();
	//_CrtCheckMemory();
    void* allocation = malloc(size);
    if(allocation) {
        InterlockedIncrement64((LONG64*)&SystemAllocationCount);
    }
	//_CrtCheckMemory();
	//time = GetTime() - time;
	//printf("Allocation size %llu, %p\n", size, allocation);
	return allocation;
}
void MemPoolSystem_Free(MemPoolSystem* pool, void* value) {
    if(!value) return;
	//printf("Freeing %p\n", value);
	//_CrtCheckMemory();
    free(value);
    InterlockedDecrement64((LONG64*)&SystemAllocationCount);
	//_CrtCheckMemory();
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
    InsideReference* holderRef = pool->holderRef;
    if(!holderRef || InterlockedCompareExchange64((LONG64*)&pool->holderRef, 0, (LONG64)holderRef) != (LONG64)holderRef) {
        // We should uniquely hold this... And if we don't it is dangerous, as this reference keeps US alive,
		//	so at this point, our own pool memory is potentially freed (as we could only exist in a redirect chain,
		//	and then just been released, so the allocation that holds us might not be anywhere on the stack).
        // So... pool->holderRef could access invalid memory at this point...
        OnError(2);
        return;
    }
    Reference_Release(&emptyReference, holderRef);
}

void* MemPoolHashed_Allocate(MemPoolHashed* pool, uint64_t size, uint64_t hash) {
    if(size != pool->VALUE_SIZE - MemPoolHashed_VALUE_OVERHEAD) {
        // Invalid input, size must equal the size the pool was initialized with (- MemPoolHashed_VALUE_OVERHEAD, which we use)
        OnError(2);
        return nullptr;
    }
	
	if (pool->countForSet.destructed) {
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
                while(true) {
                    AllocCount count = pool->countForSet;
                    AllocCount countNew = count;
                    if(countNew.destructed) {
                        // This is a valid necessary race condition, so we just have to return nullptr.
                        return nullptr;
                    }
                    countNew.totalAllocationsOutstanding++;
                    if(InterlockedCompareExchange64(
                        (LONG64*)&pool->countForSet.valueForSet,
                        countNew.valueForSet,
                        count.valueForSet
                    ) != count.valueForSet) {
                        continue;
                    }
                    break;
                }
                //printf("Added alloc to allocation %p\n", pool->holderRef);
                byte* allocation = allocatedBits + (pool->VALUE_COUNT + 7) / 8 + index * (pool->VALUE_SIZE - MemPoolHashed_VALUE_OVERHEAD);
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
    if(offset % (pool->VALUE_SIZE - MemPoolHashed_VALUE_OVERHEAD) != 0) {
        // Value isn't aligned, this isn't allowed
        OnError(3);
    }
    uint64_t index = offset / (pool->VALUE_SIZE - MemPoolHashed_VALUE_OVERHEAD);
    // Yes, might access beyond our allocation with this, but... our allocation must be in at least increments of 32 bits
    //  (at a system level), so this should be fine...
    uint32_t* pAllocated = (uint32_t*)&((byte*)pool + sizeof(MemPoolHashed))[index / 8];
    byte isAllocated = *pAllocated & (1 << (index % 8));
    if(!isAllocated) {
        // Double free? Bad, we can't always catch this...
        OnError(5);
        return;
    }

    while(true) {
        uint32_t prevValue = *pAllocated;
        uint32_t newValue = prevValue & ~(1 << (index % 8));
        if(InterlockedCompareExchange(pAllocated, newValue, prevValue) == prevValue) {
            break;
        }
    }


    while(true) {
        AllocCount count = pool->countForSet;
        AllocCount countNew = count;
        if(countNew.totalAllocationsOutstanding == 0) {
            // Double free?
            OnError(9);
            return;
        }
        countNew.totalAllocationsOutstanding--;
        if(InterlockedCompareExchange64(
            (LONG64*)&pool->countForSet.valueForSet,
            countNew.valueForSet,
            count.valueForSet
        ) != count.valueForSet) {
            continue;
        }
        if(countNew.destructed && countNew.totalAllocationsOutstanding == 0) {
            memPoolHashed_DestroyOutsideRef(pool);
        }
        break;
    }
}
// When we have no more outstanding allocations, AND are destructed, we destroy holderOutsideReference.
//  (and that's all this does, it doesn't prevent allocations or anything, I don't think that is required...)
// - The caller MUST have a reference to the allocation while calling this.
void MemPoolHashed_Destruct(MemPoolHashed* pool) {
    while(true) {
        AllocCount count = pool->countForSet;
        // Destruct may be called multiple times, as presumably the called has a reference to the allocation anyway...
        if(count.destructed) return;
        AllocCount countNew = count;
        countNew.destructed = 1;
        if(InterlockedCompareExchange64(
            (LONG64*)&pool->countForSet.valueForSet,
            countNew.valueForSet,
            count.valueForSet
        ) != count.valueForSet) {
            continue;
        }
        if(countNew.destructed && countNew.totalAllocationsOutstanding == 0) {
            memPoolHashed_DestroyOutsideRef(pool);
        }
        break;
    }
}
bool MemPoolHashed_IsInPool(MemPoolHashed* pool, void* address) {
    bool result = (uint64_t)pool <= (uint64_t)address && (uint64_t)address < (uint64_t)((byte*)pool + sizeof(MemPoolHashed) + (pool->VALUE_COUNT + 7) / 8 + pool->VALUE_COUNT * (pool->VALUE_SIZE - MemPoolHashed_VALUE_OVERHEAD));
    #ifdef DEBUG
    if(result) {
        byte* valueStart = (byte*)pool + sizeof(MemPoolHashed) + (pool->VALUE_COUNT + 7) / 8;
        if(((uint64_t)address - (uint64_t)valueStart) % (pool->VALUE_SIZE - MemPoolHashed_VALUE_OVERHEAD) != 0) {
            // address is not aligned, so... it is not something we returned, it is a random address.
            //  Are you passing random addresses? Do you think you can use a random address in the pool? You clearly can't...
            OnError(5);
        }
    }
    #endif
    return result;
}