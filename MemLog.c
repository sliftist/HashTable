#include "MemLog.h"
#include <stdio.h>


typedef struct {
    const char* file;
    uint64_t line;
    const char* operation;
    uint64_t val1;
} MemLogEntry;

uint64_t nextLogIndex;
MemLogEntry logs[1024 * 1024 * 32];

void MemLog_Reset() {
    memset(logs, 0, sizeof(MemLogEntry) * nextLogIndex);
    nextLogIndex = 0;
}

void MemLog_AddImpl(const char* file, uint64_t line, const char* operation, uint64_t val1) {
    uint64_t logIndex = InterlockedIncrement64(&nextLogIndex) - 1;
    MemLogEntry entry = { 0 };
    entry.file = file;
    entry.line = line;
    entry.operation = operation;
    entry.val1 = val1;
    logs[logIndex] = entry;
}


void MemLog_SaveValues(const char* fileName, uint64_t hash) {
    FILE* fileDescriptor = fopen(fileName, "w");

    for(uint64_t i = 0; i < nextLogIndex; i++) {
        MemLogEntry entry = logs[i];
        if(entry.val1 != hash) continue;
        fprintf(
            fileDescriptor,
            "table=%p\thash=%p\toperation=%s\tindex=%llu\n",
            entry.file,
            entry.val1,
            entry.operation,
            entry.line
        );
    }

    fclose(fileDescriptor);
}
