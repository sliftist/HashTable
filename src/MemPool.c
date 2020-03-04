#include "MemPool.h"
#include "MemPoolImpls.h"
#include "AtomicHelpers.h"
#include "environment.h"
#include "MemLog.h"

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

// TODO: We should just combine MemPoolHashedDefault with this, making the parameters typed, because the only reason for MemPoolHashedDefault is so we don't
//  need this... but we obviously need this, so... MemPoolHashedDefault is pointless.
void MemPoolHashed_Initialize(MemPoolHashed* pool) {
	//memset((byte*)pool + sizeof(MemPoolHashed) + (pool->VALUE_SIZE) * pool->VALUE_COUNT, 0, (pool->VALUE_COUNT + 7) / 8);
	// TODO: I had ambitions of only having to memset the allocated bit array, but... we need a way to detect if something has EVER been allocated,
	//  and I don't want to introduce any bugs by adding another bit array, or making the bit array contain 2 bits per, or whatever, so...
	//  I am now just memseting everything. So, todo, get this back to only memseting the bare minimum.
	//memset((byte*)pool + sizeof(MemPoolHashed) + (pool->VALUE_SIZE) * pool->VALUE_COUNT, 0, (pool->VALUE_COUNT + 7) / 8);
	memset((byte*)pool + sizeof(MemPoolHashed), 0, (pool->VALUE_SIZE) * pool->VALUE_COUNT + (pool->VALUE_COUNT + 7) / 8);
}

void* MemPoolHashed_Allocate(MemPoolHashed* pool, uint64_t size, uint64_t hash) {
	if(size != pool->VALUE_SIZE) {
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
	byte* allocatedBits = (byte*)pool + sizeof(MemPoolHashed) + (pool->VALUE_SIZE) * pool->VALUE_COUNT;
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
				byte* allocation = (byte*)pool + sizeof(MemPoolHashed) + index * (pool->VALUE_SIZE);
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

void TryToMoveUniqueIdInner(MemPoolHashed* pool, InsideReference* ref, uint64_t uniqueId, MemPools pools) {
	MemLog_Add((void*)0, ref, "checking other pools", pools.count);

	for(uint64_t i = 0; i < pools.count; i++) {
		MemPoolHashed* otherPool = pools.pools[i];
		if(otherPool == pool) continue;

		// Just for debugging...

		byte* dataStart = (byte*)otherPool + sizeof(MemPoolHashed);
		
		uint64_t baseIndex = ref->hash >> (64 - otherPool->VALUE_COUNT_LOG);
		uint64_t index = baseIndex;
		uint64_t loops = 0;

		MemLog_Add2((void*)otherPool, ref, "checking other pool", otherPool, index);

		while(true) {
			InsideReference* otherRef = (void*)(dataStart + (otherPool->VALUE_SIZE * index));
			
			// InsideReference.pool will always be set if it has ever been allocated
			if(!otherRef->pool) {
				MemLog_Add2((void*)otherPool, ref, "did not find in other pool, found never allocated", otherRef->count, otherRef);
				break;
			}

			// The other allocation might be a new allocation, or not allocated, but that is fine. If it was ever allocated,
			//  it means the unique id will always be some unique id. And if it is the newAllocation, either is has moved in,
			//  and so we can use it, or it is moving, in which case we will find it in it's location in the previous allocation anyway!

			uint64_t otherUniqueId = Reference_GetUniqueValueId(otherRef);

			MemLog_Add2((void*)otherPool, ref, "checking other pool, saw unique id at ref", otherUniqueId, otherRef);

			if(otherUniqueId == uniqueId) {
				// OH! The value can't move, because it is marked, so already deleted. Which is why the snapshot idea works.
				//  So if we see it, either that value will be unallocated, and so not a candidate, OR still allocated,
				//  and so not replacable. But it won't unallocate and reallocate in some other pool, or somewhere else...
				// ALSO! It can't be in the process of moving, because it was deleted, so we don't have to worry about uniqueId not being set...
				if(Reference_MarkIfUniqueIdEqual(otherRef, uniqueId)) {
					//breakpoint();

					MemLog_Add((void*)otherPool, ref, "gave mark away", otherRef);
					// We are single threaded here, as we are the exclusive one that is marked (and have a ref count of 0), so this is safe...
					// We don't have to worry about being marked back, as we have 0 references, so this is really only to communicate with
					//  our caller... or something...
					ref->isMarked = 0;
					return;
				} else {
					MemLog_Add((void*)otherPool, ref, "found other ref was already freed", otherRef);
				}
			}

			index = (index + 1) % otherPool->VALUE_COUNT;
			if(index == baseIndex) {
				loops++;
				if(loops > 10) {
					// Iterated way too many times
					OnError(9);
					return;
				}
			}
		}
	}
}
void TryToMoveUniqueId(MemPoolHashed* pool, InsideReference* ref) {
	uint64_t uniqueId = Reference_GetUniqueValueId(ref);
	if(uniqueId != 0) {
		MemPools pools = pool->GetAllMemPools(pool->callbackContext);
		TryToMoveUniqueIdInner(pool, ref, uniqueId, pools);
		pool->ReleaseAllMemPools(pool->callbackContext, pools);
	}
}

void MemPoolHashed_Free(MemPoolHashed* pool, void* value) {
	if(!value) return;
	uint64_t offset = (uint64_t)value - (uint64_t)((byte*)pool + sizeof(MemPoolHashed));
	if(offset % (pool->VALUE_SIZE) != 0) {
		// Value isn't aligned, this isn't allowed
		OnError(3);
	}
	uint64_t index = offset / (pool->VALUE_SIZE);
	// Yes, might access beyond our allocation with this, but... our allocation must be in at least increments of 32 bits
	//  (at a system level), so this should be fine...
	uint32_t* pAllocated = (uint32_t*)&((byte*)pool + sizeof(MemPoolHashed) + (pool->VALUE_SIZE) * pool->VALUE_COUNT)[index / 8];
	byte isAllocated = *pAllocated & (1 << (index % 8));
	if(!isAllocated) {
		// Double free? Bad, we can't always catch this...
		OnError(5);
		return;
	}

	// TODO: We should probably add a flag to skip this code if we aren't allocating references. But then... why we would even do that?
	InsideReference* ref = value;
	if(Reference_IsMarked(ref)) {
		
		// (if we even have a uniqueValueId)
		// foreach other MemPool
		//  starting from hash, while allocation ever allocated
		//	  hmm... we probably need to reserve the allocation here, to prevent it from being deallocated while we use it...
		//	  if the uniqueValueId is the same as ours, as the inside reference has references, atomically
		//		  set it to marked, failing if the count went to 0 before we could,
		//		  unsetting our own uniqueValueId (first?), and then breaking upon success
		// If we are still marked, then actually destruct, otherwise, another version of our value in another table
		//	  must have been marked, and so it will deal with this once it destructs

		uint64_t uniqueId = Reference_GetUniqueValueId(ref);
		MemLog_Add2((void*)0, ref, "MemPool freeing, saw marked, uniqueId=", ref->hash, uniqueId);
		if(uniqueId != 0) {
			TryToMoveUniqueId(pool, ref);
		}
		if(Reference_IsMarked(ref)) {
			MemLog_Add((void*)0, ref, "MemPool freeing, saw marked, still marked, calling callback", ref->hash);

			// If we are still marked... then it means we couldn't move our mark, so... we can actually free
			// And obviously, if we can't move it, then there are no references anywhere, so no new ones will pop up, this reference
			//  is absolutely and completely dead.
			pool->FreeCallback(pool->callbackContext, (byte*)value + InsideReferenceSize);
		}
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
			pool->OnNoMoreAllocations(pool->callbackContext);
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
			pool->OnNoMoreAllocations(pool->callbackContext);
		}
		break;
	}
}
bool MemPoolHashed_IsInPool(MemPoolHashed* pool, void* address) {
	bool result = (uint64_t)pool <= (uint64_t)address && (uint64_t)address < (uint64_t)((byte*)pool + sizeof(MemPoolHashed) + pool->VALUE_COUNT * (pool->VALUE_SIZE));
	#ifdef DEBUG
	if(result) {
		byte* valueStart = (byte*)pool + sizeof(MemPoolHashed);
		if(((uint64_t)address - (uint64_t)valueStart) % (pool->VALUE_SIZE) != 0) {
			// address is not aligned, so... it is not something we returned, it is a random address.
			//  Are you passing random addresses? Do you think you can use a random address in the pool? You clearly can't...
			OnError(5);
		}
	}
	#endif
	return result;
}