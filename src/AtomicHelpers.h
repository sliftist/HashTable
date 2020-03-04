#pragma once

// Returns true on success
bool InterlockedCompareExchangeStruct128(
	void* structAddress,
	void* structOriginal,
	void* structNew
);