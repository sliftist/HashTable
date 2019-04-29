#include "Timing.h"

TimeTrackerRoot rootTimer = { 0 };

uint64_t GetTime() {
    //unsigned int cpuid;
	//return __rdtscp(&cpuid);
	return __rdtsc();
}

void Timing_StartInner(TimeTrackerRoot* root, TimeTracker* tracker, const char* name) {
    //if(root->startTime) return;
    if(!tracker->inRoot) {
        if(!root->startTime) return;
        tracker->inRoot = 1;
        tracker->name = name;
        tracker->totalTime = 0;
        tracker->totalCount = 0;

        if(root->nextTrackerIndex >= TimeTrackerRoot_trackers_count) {
            // Ran out of root tracker slots
            OnError(4);
        } else {
            root->trackers[root->nextTrackerIndex++] = tracker;
        }
    }

	uint64_t start = GetTime();
    if(tracker->currentStartTime) {
        // Start called before End
        OnError(3);
    }
    tracker->currentStartTime = start;
}
void Timing_End(TimeTracker* tracker) {
    if(!tracker->inRoot) return;
	uint64_t end = GetTime();
    if(!tracker->currentStartTime) {
        // End called before start
        OnError(3);
        return;
    }
    uint64_t time = end - tracker->currentStartTime;
    tracker->currentStartTime = 0;
    InterlockedAdd64(&tracker->totalTime, time);
    InterlockedIncrement64(&tracker->totalCount);
}