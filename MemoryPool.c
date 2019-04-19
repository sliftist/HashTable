#include "MemoryPool.h"

void* MemoryPool_Allocate(MemoryPool* this) {
    for(uint64_t i = 0; i < MEMORY_POOL_MAX_VALUES; i++) {
        void* value = this->values[i];
        if(value) {
            if(InterlockedCompareExchange64(
                (LONG64*)&this->values[i],
                nullptr,
                (LONG64)value
            ) == (LONG64)value) {
                return value;
            }
        }
    }

    return malloc(this->SIZE);
}
void MemoryPool_Free(MemoryPool* this, void* value) {
    if(!this) {
        free(value);
        return;
    }
    for(uint64_t i = 0; i < MEMORY_POOL_MAX_VALUES; i++) {
        if(!this->values[i]) {
            if(InterlockedCompareExchange64(
                (LONG64*)&this->values[i],
                (LONG64)value,
                nullptr
            ) == nullptr) {
                return;
            }
        }
    }
    free(value);
}