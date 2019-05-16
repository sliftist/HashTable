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
uint64_t logCount = 1024 * 1024 * 128;
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
            "table=%p\thash=%p\toperation=%s\tindex=%llu\n",
            entry.file,
            entry.val1,
            entry.operation,
            entry.line
        );
    }

    fclose(fileDescriptor);
}

void MemLog_SaveValuesTable(const char* fileName, void* table, uint64_t mallocId, uint64_t hash) {
    FILE* fileDescriptor = fopen(fileName, "w");

    for(uint64_t i = 0; i < (nextLogIndex % (logCount)); i++) {
        MemLogEntry entry = logs[i];
        if(entry.file == table && (entry.val1 == hash || entry.val1 == 0)
            && abs((int64_t)(entry.mallocId - mallocId)) <= 1
        ) {
            fprintf(
                fileDescriptor,
                "table=%p(%lld)\thash=%p\toperation=%s\tindex=%llu\tval=%llu\n",
                entry.file,
                (int64_t)(entry.mallocId - mallocId),
                entry.val1,
                entry.operation,
                entry.line,
                entry.val2
            );
        }
    }

    fclose(fileDescriptor);
}
