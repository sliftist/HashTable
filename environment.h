#pragma once

#include <Windows.h>
#include <stdint.h>

#define PAGE_SIZE 4096

// malloc
// free
// nullptr
// bool, true, false

#define bool int
#define true 1
#define false 0

#define nullptr 0

#define CASSERT(predicate) typedef char C_STATIC_ASSERT_blah [2*!!(predicate)-1];

// InterlockedCompareExchange
// InterlockedCompareExchange16
// InterlockedIncrement

#ifdef __cplusplus
extern "C" {
#endif

void OnError(int code);

#ifdef __cplusplus
}
#endif