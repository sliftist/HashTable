#include "environment.h"
#include "TransactionQueueHelpers.h"
#include "AtomicHelpers.h"


int AtomicSet(AtomicUnit* unit, TransactionChange change) {
	while (true) {
		AtomicUnit unitValue = *unit;
		if (unitValue.transactionId > change.transactionId) {
			return 1;
		}
		AtomicUnit newValue = unitValue;
		newValue.transactionId = change.transactionId;
		newValue.value = change.newValue;
		if (InterlockedCompareExchangeStruct128(
			unit,
			&unitValue,
			&newValue
		)) {
			break;
		}
	}
	return 0;
}

int ApplyStructChange(void* context, TransactionChange change) {
	AtomicUnit* units = context;

	uint64_t dataIndex = TransactionChange_get_dataIndex(&change);

	AtomicSet(units + dataIndex, change);

	return 0;
}

// TODO: We need an accessor for arbitrary memory, and then "overloads" for most types.


int Set_Bytes(
	void* insertWriteContext,
	int(*insertWrite)(void* context, TransactionChange change),
	AtomicUnit* units,
	void* offset,
	unsigned char* values,
	uint64_t valuesCount
) {
	// We could change this to work with different amounts of bytes, but we would have to change it from uint32_t.
	CASSERT(BYTES_PER_TRANSACTION == 4);

	uint64_t bytesOffset = (uint64_t)offset - (uint64_t)units;

	uint64_t alignedOffsetStart = bytesOffset / 4 * 4;
	uint64_t alignedOffsetEnd = (bytesOffset + valuesCount + 3) / 4 * 4;

	for (uint64_t pos = alignedOffsetStart; pos < alignedOffsetEnd; pos += 4) {

		uint64_t valuesIndexStart = pos - bytesOffset;
		
		// TODO: For all but the last and first iteration this can be simplified to
		//uint32_t value = *(uint32_t*)(values + valuesIndexStart);

		uint32_t value = (uint32_t)units[pos / 4].value;
		for (int offset = 0; offset < 4; offset++) {
			uint64_t valuesIndex = valuesIndexStart + offset;
			if (valuesIndex < 0) continue;
			if (valuesIndex >= valuesCount) continue;
			unsigned char byte = values[valuesIndex];

			value = value & ~(0xFF << (offset * 8)) | (byte << (offset * 8));
		}

		TransactionChange change;
		TransactionChange_set_dataIndex(&change, pos / 4);
		change.newValue = value;

		int offset = insertWrite(insertWriteContext, change);
		if (offset != 0) {
			return offset;
		}
	}
	return 0;
}

void Get_Bytes(
	AtomicUnit* units,
	void* offset,
	unsigned char* values,
	uint64_t valuesCount
) {
	// We could change this to work with different amounts of bytes, but we would have to change it from uint32_t.
	CASSERT(BYTES_PER_TRANSACTION == 4);

	uint64_t bytesOffset = (uint64_t)offset - (uint64_t)units;

	uint64_t alignedOffsetStart = bytesOffset / 4 * 4;
	uint64_t alignedOffsetEnd = (bytesOffset + valuesCount + 3) / 4 * 4;

	for (uint64_t pos = alignedOffsetStart; pos < alignedOffsetEnd; pos += 4) {

		uint64_t valuesIndexStart = pos - bytesOffset;
		uint32_t value = (uint32_t)units[pos / 4].value;

		// TODO: For all but the last and first iteration this can be simplified to
		//*(uint32_t*)(values + valuesIndexStart) = value;

		for (int offset = 0; offset < 4; offset++) {
			uint64_t valuesIndex = valuesIndexStart + offset;
			if (valuesIndex < 0) continue;
			if (valuesIndex >= valuesCount) continue;

			unsigned char byte = (value >> (offset * 8)) & 0xFF;
			values[valuesIndex] = byte;
		}
	}
}



// Only call this inside the callback passed ApplyWrite, and if the result to this function is non-zero, return that
//	as the response to that callback.

#define Set_X_impl(type) \
int Set_##type( \
	void* insertWriteContext, \
	int (*insertWrite)(void* context, TransactionChange change), \
	AtomicUnit* units, \
	/* Should be offset in comparison to units */ \
	type* offset, \
	type value \
) { \
	return Set_Bytes( \
		insertWriteContext, \
		insertWrite, \
		units, \
		offset, \
		(unsigned char*)&value, \
		sizeof(type) \
	); \
}

#define Get_X_impl(type) \
type Get_##type( \
	AtomicUnit* units, \
	type* offset \
) { \
	type output; \
	Get_Bytes( \
		units, \
		offset, \
		(unsigned char*)&output, \
		sizeof(type) \
	); \
	return output; \
}

Set_X_impl(int32_t)
Get_X_impl(int32_t)