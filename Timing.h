#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "environment.h"

#define TimeTrackerRoot_trackers_count 20

typedef struct {
    bool inRoot;
	uint64_t totalTime;
	uint64_t totalCount;
	uint64_t currentStartTime;
    const char* name;
} TimeTracker;

typedef struct {
    uint64_t startTimeNanoSeconds;
    uint64_t startTime;
    TimeTracker* trackers[TimeTrackerRoot_trackers_count];
    uint64_t nextTrackerIndex;
} TimeTrackerRoot;

extern TimeTrackerRoot rootTimer;

#define TimeBlock(name, code) \
static TimeTracker TRACKER##name = { 0 }; \
Timing_StartInner(&rootTimer, &TRACKER##name, #name); \
code \
Timing_End(&TRACKER##name);

#undef TimeBlock
#define TimeBlock(name, code) code

//#define FAST_UNSAFE_CHANGES 1


#define Timing_Start(root, tracker) Timing_StartInner(root, tracker, #tracker)
void Timing_StartInner(TimeTrackerRoot* root, TimeTracker* tracker, const char* name);
void Timing_End(TimeTracker* tracker);
uint64_t GetTime();


#ifdef __cplusplus
}
#endif