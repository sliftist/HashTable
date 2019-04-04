#include <list>

#include "AtomicHashTable.h"

PointersSnapshotDelta Debug_ComparePointersSnapshot(PointersSnapshot* before) {
	PointersSnapshot* after = Debug_GetPointersSnapshot();
	std::list<SmallPointerEntry_Extended> afterValues;
	for (uint64_t i = 0; i < after->entryCount; i++) {
		afterValues.push_front(after->entries[i]);
	}
	std::list<SmallPointerEntry_Extended> beforeValues;
	for (uint64_t i = 0; i < before->entryCount; i++) {
		beforeValues.push_front(before->entries[i]);
	}

	auto ptrFind = [&](SmallPointerEntry_Extended beforeEntry) {
		for (auto it = afterValues.begin(); it != afterValues.end(); it++) {
			auto afterValue = *it;
			if (afterValue.pointer == beforeEntry.pointer && afterValue.size == beforeEntry.size) {
				afterValues.erase(it);
				return true;
			}
		}
		return false;
	};
	for (auto pBeforeEntry = beforeValues.begin(); pBeforeEntry != beforeValues.end();) {
		SmallPointerEntry_Extended beforeEntry = *pBeforeEntry;
		auto pAfterEntry = pBeforeEntry;

		if (ptrFind(beforeEntry)) {
			beforeValues.erase(pBeforeEntry++);
		}
		else {
			pBeforeEntry++;
		}
	}

	auto valueFind = [&](SmallPointerEntry_Extended beforeEntry) {
		for (auto it = afterValues.begin(); it != afterValues.end(); it++) {
			auto afterValue = *it;
			if (afterValue.size == beforeEntry.size) {
				afterValues.erase(it);
				return true;
			}
		}
		return false;
	};
	for (auto pBeforeEntry = beforeValues.begin(); pBeforeEntry != beforeValues.end();) {
		SmallPointerEntry_Extended beforeEntry = *pBeforeEntry;
		auto pAfterEntry = pBeforeEntry;

		if (valueFind(beforeEntry)) {
			beforeValues.erase(pBeforeEntry++);
		}
		else {
			pBeforeEntry++;
		}
	}


	PointersSnapshotDelta delta = { 0 };
	for (auto pEntry = beforeValues.begin(); pEntry != beforeValues.end(); pEntry++) {
		delta.changeType = 1;
		delta.entry = *pEntry;
		printf("Added count %llu, remove count %llu\n", afterValues.size(), beforeValues.size());

		/*
		printf("size %llu, %llu\n", beforeValues.begin()->size, afterValues.begin()->size);
		printf("at %s:%llu, %s:%llu\n",
			beforeValues.begin()->fileName,
			beforeValues.begin()->line,
			afterValues.begin()->fileName,
			afterValues.begin()->line
		);
		*/
		break;
	}
	for (auto pEntry = afterValues.begin(); pEntry != afterValues.end(); pEntry++) {
		delta.changeType = 2;
		delta.entry = *pEntry;
		printf("Added count %llu, remove count %llu\n", afterValues.size(), beforeValues.size());
		break;
	}
	
	//Debug_DeallocatePointersSnapshot(after);
	//Debug_DeallocatePointersSnapshot(before);

	return delta;
}