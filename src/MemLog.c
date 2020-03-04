#include "MemLog.h"
#include <stdio.h>

#include "AtomicHashTable2.h"


typedef struct {
	const char* file;
	uint64_t line;
	const char* operation;
	uint64_t val1;
	uint64_t val2;

	uint64_t mallocId;
} MemLogEntry;

uint64_t nextLogIndex;
// 12GB... and we actually need it. We are going to have to start getting rid of unneeded logs soon...
uint64_t logCount = 1024 * 1024 * 256;
MemLogEntry* logs = nullptr;

void MemLog_Init() {
	MemLog_Reset();
	logs = malloc(logCount * sizeof(MemLogEntry));
	memset(logs, 0, logCount * sizeof(MemLogEntry));
}

void MemLog_Reset() {
	if(logs) {
		free(logs);
		logs = nullptr;
	}
	nextLogIndex = 0;
}

void MemLog_AddImpl(const char* file, uint64_t line, const char* operation, uint64_t val1, uint64_t val2) {
	uint64_t logIndex = (InterlockedIncrement64(&nextLogIndex) - 1) % (logCount);
	MemLogEntry entry = { 0 };
	entry.file = file;
	entry.line = line;
	entry.operation = operation;
	entry.val1 = val1;
	entry.val2 = val2;

	if(file) {
		entry.mallocId = ((AtomicHashTableBase*)file)->mallocId;
	}

	logs[logIndex] = entry;
}


void MemLog_SaveValues(const char* fileName, uint64_t hash) {
	FILE* fileDescriptor = fopen(fileName, "w");

	for(uint64_t i = 0; i < (nextLogIndex % (logCount)); i++) {
		MemLogEntry entry = logs[i];
		if(entry.val1 != hash) continue;
		fprintf(
			fileDescriptor,
			"table=%p(%p)\thash=%p\toperation=%s\tindex=%p\n",
			entry.file,
			entry.mallocId,
			entry.val1,
			entry.operation,
			entry.line
		);
	}

	fclose(fileDescriptor);
}

void MemLog_SaveValuesIndex(const char* fileName, uint64_t index) {
	FILE* fileDescriptor = fopen(fileName, "w");

	//*
	uint64_t uniqueIdValue = 10;
	uint64_t nextIndex = 10;
	uint64_t startIndex = 0;
	for(uint64_t i = 0; i < (nextLogIndex % (logCount)); i++) {
		MemLogEntry entry = logs[i];
		if(entry.line != index) continue;
		if(entry.operation == (void*)"trailing ref unique id new ref + uniqueValueId") {
			startIndex = i;
			uniqueIdValue = entry.val2;
			nextIndex = entry.val1;
		}
	}
	//*/
	

	bool start = false;
	for(uint64_t i = startIndex; i < (nextLogIndex % (logCount)); i++) {
		MemLogEntry entry = logs[i];
		if(entry.line == index
		|| entry.line == nextIndex || entry.val2 == uniqueIdValue
		) {
			if(entry.operation == (void*)"trailing ref unique id new ref + uniqueValueId") {
				start = true;
			}
			if(!start) continue;
			if(entry.operation == (void*)"MemPool freeing, saw marked, uniqueId=" && entry.line == nextIndex) {
				fprintf(fileDescriptor, "----------- entering dtor of new ref, should realize old ref is still in a table, and not decide to free it -----------\n");
			}
			fprintf(
				fileDescriptor,
				"table=%p(%p)\thash=%p\toperation=%s\tindex=%p\n",
				entry.file,
				entry.val2,
				entry.val1,
				entry.operation,
				entry.line
			);
		}
	}

	fclose(fileDescriptor);
}

void MemLog_SaveValuesIndexMulti(const char* fileName, ...) {
	va_list args;
	va_start(args, fileName);

	FILE* fileDescriptor = fopen(fileName, "w");
	uint64_t indexes[100] = { 0 };
	uint64_t indexCount = 0;
	while(true) {
		indexes[indexCount] = va_arg(args, uint64_t);
		if (!indexes[indexCount]) break;
		indexCount++;
	}


	for(uint64_t i = 0; i < (nextLogIndex % (logCount)); i++) {
		MemLogEntry entry = logs[i];
		bool found = false;
		for(uint64_t k = 0; k < indexCount; k++) {
			if(entry.line == indexes[k]) {
				found = true;
				break;
			}
		}
		if(!found) continue;
		fprintf(
			fileDescriptor,
			"table=%p\thash=%p\toperation=%s\tindex=%p\tval=%p\n",
			entry.file,
			entry.val1,
			entry.operation,
			entry.line,
			entry.val2
		);
	}

	fclose(fileDescriptor);
}

void MemLog_SaveValuesTable(const char* fileName, void* table) {
	FILE* fileDescriptor = fopen(fileName, "w");

	for(uint64_t i = 0; i < (nextLogIndex % (logCount)); i++) {
		MemLogEntry entry = logs[i];
		if(entry.file != table) continue;
		fprintf(
			fileDescriptor,
			"table=%p\thash=%p\toperation=%s\tindex=%llu\tval=%llu\n",
			entry.file,
			entry.val1,
			entry.operation,
			entry.line,
			entry.val2
		);
	}

	fclose(fileDescriptor);
}

void MemLog_SaveValuesOperation(const char* fileName, void* operation) {
	FILE* fileDescriptor = fopen(fileName, "w");

	for(uint64_t i = 0; i < (nextLogIndex % (logCount)); i++) {
		MemLogEntry entry = logs[i];
		if(entry.operation != operation) continue;
		fprintf(
			fileDescriptor,
			"table=%p(%p)\thash=%p\toperation=%s\tindex=%p\n",
			entry.file,
			entry.mallocId,
			entry.val1,
			entry.operation,
			entry.line
		);
	}

	fclose(fileDescriptor);
}
