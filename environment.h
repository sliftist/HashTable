#pragma once

#define CASSERT(predicate) typedef char C_STATIC_ASSERT_blah [2*!!(predicate)-1];

#if !defined(NDEBUG) && defined(_DEBUG)
#define DEBUG
#endif

#ifdef DEBUG
#ifdef _MSC_VER
#define breakpoint() __debugbreak()
#define WINDOWS
#else
#define breakpoint() asm("int $3")
#endif
#else
#define breakpoint() ;
#endif


#ifdef KERNEL

	#include <stdint.h>

	#undef InterlockedIncrement64
	#define InterlockedIncrement64(x) _InterlockedIncrement64((LONG64*)x)

	#undef InterlockedCompareExchange64
	#define InterlockedCompareExchange64(x, y, z) _InterlockedCompareExchange64((LONG64*)(x), y, z)

	#undef nullptr
	#define nullptr 0

#else

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

	// InterlockedCompareExchange
	// InterlockedCompareExchange16
	// InterlockedIncrement

#endif


#ifdef __cplusplus
extern "C" {
#endif


void OnErrorInner(int code, const char* name, unsigned long long line);
#define OnError(code) breakpoint(); OnErrorInner(code, __FILE__, __LINE__)

#define ErrorTop(statement) { int errorTopResult = statement; if(errorTopResult != 0) { OnError(errorTopResult); } }


#ifdef __cplusplus
}
#endif