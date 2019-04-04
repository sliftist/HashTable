#pragma once

#include "TransactionQueue.h"

#ifdef __cplusplus
extern "C" {
#endif



#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
	uint64_t transactionId : 60;
	uint64_t value : BITS_PER_TRANSACTION;
} AtomicUnit;
#pragma pack(pop)

CASSERT(sizeof(AtomicUnit) == 16);


int AtomicSet(AtomicUnit* unit, TransactionChange change);

int ApplyStructChange(void* context, TransactionChange change);

// Only call this inside the callback passed ApplyWrite, and if the result to this function is non-zero, return that
//	as the response to that callback.

int Set_Bytes(
	void* insertWriteContext,
	int(*insertWrite)(void* context, TransactionChange change),
	AtomicUnit* units,
	void* offset,
	unsigned char* values,
	uint64_t valuesCount
);

void Get_Bytes(
	AtomicUnit* units,
	void* offset,
	unsigned char* values,
	uint64_t valuesBytes
);

#define Set_X(type) \
int Set_##type( \
	void* insertWriteContext, \
	int(*insertWrite)(void* context, TransactionChange change), \
	AtomicUnit* units, \
	type* offset, \
	type value \
);

#define Get_X(type) \
type Get_##type( \
	AtomicUnit* units, \
	type* offset \
);

Set_X(int32_t)
Get_X(int32_t)

Set_X(uint32_t)
Get_X(uint32_t)

Set_X(uint64_t)
Get_X(uint64_t)


#ifdef __cplusplus
}
#endif