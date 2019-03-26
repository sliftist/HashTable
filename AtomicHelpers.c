#include "environment.h"

bool InterlockedCompareExchangeStruct128
(
	void* structAddress,
	void* structOriginal,
	void* structNew
) {
	int zero = 0;
	if ((LONG64)structAddress % 16 != 0) {
		zero = 1 / zero;
	}
	if ((LONG64)structOriginal % 16 != 0) {
		zero = 1 / zero;
	}
	if ((LONG64)structNew % 16 != 0) {
		zero = 1 / zero;
	}
	return InterlockedCompareExchange128(
		(LONG64*)structAddress,
		((LONG64*)structNew)[1],
		((LONG64*)structNew)[0],
		(LONG64*)structOriginal
	);
}



bool EqualsStruct128(
	void* structLhs,
	void* structRhs
) {
	// If the compare succeeds, it means lhs == rhs (and the set doesn't matter, as it only sets if they are equal,
	//	which then is a noop), and on success InterlockedCompareExchange128 returns 1, so this works.
	return InterlockedCompareExchangeStruct128(
		structLhs,
		structRhs,
		structLhs
	);
}