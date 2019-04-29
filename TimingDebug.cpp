#include <chrono>

#include "TimingDebug.h"
#include "Environment.h"
#include "Timing.h"

void Timing_StartRoot(TimeTrackerRoot* root) {
	root->startTime = GetTime();
    if(root->startTimeNanoSeconds) {
        // Start called before End
        OnError(3);
    }
    root->startTimeNanoSeconds = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    root->nextTrackerIndex = 0;
}

void Timing_EndRootPrint(TimeTrackerRoot* root, uint64_t iterationCount) {
    uint64_t endTime = GetTime();

    // remember to reset all inRoots of all trackers
	uint64_t endTimeNs = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    uint64_t nsDuration = endTimeNs - root->startTimeNanoSeconds;

    uint64_t totalTime = endTime - root->startTime;


    uint64_t allCounts = 0;
    for(uint64_t i = 0; i < root->nextTrackerIndex; i++) {
        TimeTracker tracker = *root->trackers[i];
        allCounts += tracker.totalCount;
    }


    uint64_t nsPerTimeOverhead = 0;
    uint64_t nsInTimeOverhead = 0;
    {
        uint64_t testCount = 1000 * 1000;
        TimeTrackerRoot testRoot = { 0 };
        TimeTracker test = { 0 };
        Timing_StartRoot(&testRoot);
        for(uint64_t i = 0; i < testCount; i++) {
            Timing_Start(&testRoot, &test);
            Timing_End(&test);
        }

        uint64_t testEndTime = GetTime();
	    uint64_t testEndTimeNs = std::chrono::high_resolution_clock::now().time_since_epoch().count();

        nsPerTimeOverhead = (testEndTimeNs - testRoot.startTimeNanoSeconds) / testCount;
        nsInTimeOverhead = (uint64_t)((double)(test.totalTime) / (testEndTime - testRoot.startTime) * (testEndTimeNs - testRoot.startTimeNanoSeconds) / testCount);
    }

    //uint64_t realTotalTime = totalTime - nsPerTimeOverhead * allCounts;

    uint64_t nsOverheadEstimate = nsPerTimeOverhead * allCounts;

    printf("%f seconds, %f%% overhead, %lluns per, %lluns overhead in time per, time per operation %fns, count %llu\n",
        (double)nsDuration / (1000 * 1000 * 1000),
        (double)nsOverheadEstimate / nsDuration * 100,
        nsPerTimeOverhead,
        nsInTimeOverhead,
		((double)nsDuration / iterationCount),
		iterationCount
	);
    for(uint64_t i = 0; i < root->nextTrackerIndex; i++) {
        TimeTracker tracker = *root->trackers[i];
        printf("\t%s, %f%%, approx time per %fns, count %llu\n",
            tracker.name,
            ((double)tracker.totalTime) / totalTime * 100,
            (double)tracker.totalTime / totalTime * nsDuration / tracker.totalCount,
            tracker.totalCount
        );
    }


    root->startTimeNanoSeconds = 0;
    for(uint64_t i = 0; i < root->nextTrackerIndex; i++) {
        root->trackers[i]->inRoot = 0;
    }
    root->nextTrackerIndex = 0;
}