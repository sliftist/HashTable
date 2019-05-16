#pragma once

#include "environment.h"

#ifdef __cplusplus
extern "C" {
#endif

//todonext
// Add an init function and dynamically allocate the log, because we need way more entries.
//  And then also... maybe log less? Well in debug mode it took 607 million entries before we encountered the bug...
//  so either we log less, or do it in release, because release seems to encounter the bug faster...
void MemLog_Init();

void MemLog_Reset();
#ifdef DEBUG
#define MemLog_Add(file, line, operation, val1) MemLog_AddImpl(file, line, operation, val1, 0)
#define MemLog_Add2(file, line, operation, val1, val2) MemLog_AddImpl(file, line, operation, val1, val2)
#else
//#define MemLog_Add(file, line, operation, val1) MemLog_AddImpl(file, line, operation, val1, 0)
//#define MemLog_Add2(file, line, operation, val1, val2) MemLog_AddImpl(file, line, operation, val1, val2)
#define MemLog_Add(file, line, operation, val1) false
#define MemLog_Add2(file, line, operation, val1, val2) false
#endif
void MemLog_AddImpl(const char* file, uint64_t line, const char* operation, uint64_t val1, uint64_t val2);

void MemLog_SaveValues(const char* fileName, uint64_t hash);
void MemLog_SaveValuesTable(const char* fileName, void* table, uint64_t mallocId, uint64_t hash);

#ifdef __cplusplus
}
#endif