#include "BulkAlloc.h"

#include "AtomicHelpers.h"

// Must call this single threaded...
void BulkAlloc_ctor(BulkAlloc* this, uint64_t allocSize) {
    if(InterlockedCompareExchange(
        (LONG*)&this->initialized,
        true,
        false
    )) {
        // Already initialized
        OnError(3);
        return;
    }
    this->size = allocSize + sizeof(BulkAlloc_AllocHead);
}

bool getIsLastAllocation(BulkAlloc* this, uint64_t allocIndex) {
    return allocIndex == (BulkAlloc_AllocationCount - 1) || this->allocations[allocIndex + 1].data.allocation == nullptr;
}
void* BulkAlloc_alloc(BulkAlloc* this) {
    if(!this->initialized) {
        // Not initialized
        OnError(3);
    }
    uint64_t size = this->size;

    uint64_t startAllocationIndex = this->allocSearchIndex;

    {
        BulkAlloc_Alloc* firstAlloc = &this->allocations[0].data;
        if(!firstAlloc->allocation) {
            BulkAlloc_Alloc alloc = { 0 };
            alloc.count = 128;
            alloc.allocation = malloc(alloc.count * this->size);
            if(!alloc.allocation) {
                // Out of memory
                OnError(3);
                return nullptr;
            }
            memset(alloc.allocation, 0, alloc.count * this->size);
            BulkAlloc_Alloc zeroAlloc = { 0 };
            if(!InterlockedCompareExchangeStruct128(
                firstAlloc,
                &zeroAlloc,
                &alloc
            )) {
                free(alloc.allocation);
            }
        }
    }

    for(uint64_t offset = 0; offset < BulkAlloc_AllocationCount; offset++) {
        uint64_t allocIndex = (startAllocationIndex + offset) % BulkAlloc_AllocationCount;

        // size/bytesUsed may not be in sync with allocation, because this is a 128 bit read... but
        //  size/bytesUsed should be in sync with each, as 64 bit reads should be threadsafe.
        BulkAlloc_AllocFull* pAlloc = &this->allocations[allocIndex];
        if(!pAlloc->data.allocation) continue;

        bool isLastAllocation = getIsLastAllocation(this, allocIndex);

        InterlockedIncrement((LONG*)&pAlloc->data.countUsed);
        if(!isLastAllocation && (pAlloc->data.count / pAlloc->data.countUsed < 2)) {
            InterlockedDecrement((LONG*)&pAlloc->data.countUsed);
            // Don't reuse old allocations until they become empty enough. Instead prefer to allocation more allocations,
            //  that way unless allocations are 
            continue;
        }

        // Allow a start of searchIndex, because we saw that more than 50% of bytes were free (recently, but maybe not anymore...)
        uint64_t searchIndex = pAlloc->searchIndex;
        if(searchIndex == pAlloc->data.count) {
            InterlockedCompareExchange64(
                (LONG64*)&pAlloc->searchIndex,
                0,
                pAlloc->data.count
            );
            searchIndex = 0;
        }
        
        for(uint64_t i = searchIndex; i < pAlloc->data.count; i++) {
            BulkAlloc_AllocHead* head = (void*)&pAlloc->data.allocation[i * size];
            
            if(!head->allocated) {
                if(InterlockedCompareExchange(
                    (LONG*)&head->allocated,
                    1,
                    0
                ) != 0) {
                    continue;
                } else {
                    // Each hanging thread can reset a search of an allocation... but that should be fine
                    pAlloc->searchIndex = i + 1;

                    // This isn't perfect, but it should make it more difficult for hanging threads to mess up allocSearchIndex...
                    //  which doesn't break anything, it just slows down allocations.
                    InterlockedCompareExchange64(
                        (LONG64*)&this->allocSearchIndex,
                        allocIndex,
                        startAllocationIndex
                    );

                    return (void*)&head[1];
                }
            }
        }

        // If we got here it means we couldn't find a free spot
        InterlockedDecrement((LONG*)&pAlloc->data.countUsed);

        if(isLastAllocation) {
            if(allocIndex == BulkAlloc_AllocationCount) {
                // Ran out of memory...
                OnError(3);
                return nullptr;
            }
            BulkAlloc_Alloc* pNextAlloc = &this->allocations[allocIndex + 1].data;
            if(pNextAlloc->allocation) {
                // If there is a new allocation, we can just continue and automatically use it...
                continue;
            }
            uint64_t allocCount = (uint64_t)this->allocations[allocIndex].data.count * 2;
            if(allocCount > (uint64_t)UINT32_MAX) {
                allocCount = UINT32_MAX;
            }
            void* newAllocation = malloc(size * allocCount);
            if(!newAllocation) {
                if(pNextAlloc->allocation) {
                    // Many threads probably raced to allocate this, which is why we failed to allocate, but someone
                    //  allocated and shared the allocation, so... we can just continue
                    continue;
                }
                // Just out of memory...
                OnError(3);
                return nullptr;
            }
            memset(newAllocation, 0, size * allocCount);
            BulkAlloc_Alloc prevAlloc = { 0 };

            BulkAlloc_Alloc alloc = { 0 };
            alloc.count = (uint32_t)allocCount;
            alloc.allocation = newAllocation;

            if(!InterlockedCompareExchangeStruct128(
                pNextAlloc,
                &prevAlloc,
                &alloc
            )) {
                free(newAllocation);
                continue;
            }
        }
    }

    // Out of allocations indexes, which seems implausible...
    OnError(13);
    return nullptr;
}

void BulkAlloc_free(BulkAlloc* this, void* pointer) {
    for(uint64_t i = 0; i < BulkAlloc_AllocationCount; i++) {
        // Nothing we are reading needs to be atomic, so this read is fine...
        BulkAlloc_Alloc* pAlloc = &this->allocations[i].data;
        if(!pAlloc->allocation) continue;

        // If it becomes negative it wraps around and becomes large, which just makes it not match, so it is fine...
        uint64_t byteOffset = (uint64_t)pointer - (uint64_t)pAlloc->allocation - sizeof(BulkAlloc_AllocHead);
        if(byteOffset >= pAlloc->count * this->size) continue;

        if(byteOffset % this->size != 0) {
            // Pointer isn't aligned, so what is it?
            OnError(3);
        }

        BulkAlloc_AllocHead* head = (void*)&pAlloc->allocation[byteOffset];
        if(InterlockedCompareExchange(
            (LONG*)&head->allocated,
            0,
            1
        ) != 1) {
            // Double free, or never allocated in the first place
            OnError(3);
        }
        InterlockedDecrement((LONG*)&pAlloc->countUsed);

        break;
    }
}

// Must be single threaded by this point...
void BulkAlloc_dtorBase(BulkAlloc* this, bool ignoreLeaks) {
    if(!InterlockedCompareExchange(
        (LONG*)&this->initialized,
        false,
        true
    )) {
        // Not initialized
        OnError(3);
        return;
    }

    uint64_t size = this->size;
    for(uint64_t a = 0; a < BulkAlloc_AllocationCount; a++) {
        BulkAlloc_Alloc* pAlloc = &this->allocations[a].data;
        if(!pAlloc->allocation) continue;
        if(!ignoreLeaks) {
            for(uint64_t i = 0; i < pAlloc->count; i++) {
                BulkAlloc_AllocHead* head = (void*)&pAlloc->allocation[i * size];
                if(head->allocated) {
                    // Leaked allocation
                    OnError(4);
                }
            }
        }
        free(pAlloc->allocation);
    }
    memset(this, 0, sizeof(BulkAlloc));
}

void BulkAlloc_dtor(BulkAlloc* this) {
    BulkAlloc_dtorBase(this, false);
}
void BulkAlloc_dtorIgnoreLeaks(BulkAlloc* this) {
    BulkAlloc_dtorBase(this, true);
}