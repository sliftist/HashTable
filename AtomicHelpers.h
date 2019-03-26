#pragma once

bool InterlockedCompareExchangeStruct128(
	void* structAddress,
	void* structOriginal,
	void* structNew
);

bool EqualsStruct128(
	void* structLhs,
	void* structRhs
);