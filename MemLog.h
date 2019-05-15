#pragma once

#include "environment.h"

#ifdef __cplusplus
extern "C" {
#endif

void MemLog_Reset();
void MemLog_AddImpl(const char* file, uint64_t line, const char* operation, uint64_t val1);
void MemLog_SaveValues(const char* fileName);

#ifdef __cplusplus
}
#endif