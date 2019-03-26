// https://github.com/skarupke/flat_hash_map/blob/master/flat_hash_map.hpp,
//	but in C, stripping out a lot of code, and 

// Actually... just make our own algorithm. Take a size_t hash, and use a size fixed
//	by define, and then put all of our functions in a define so we can create functions
//	per value size, with custom names.
// Use PAGE_SIZE
// Make it thread safe with InterlockedIncrement, etc being defined in a platform independent header,
//	which we can probably define correctly for compilation here, and which we can of course define
//	for the windows kernel.
//	- Also make it thread safe when expanding the size, without adding a lot of locking
// Make it faster for items that don't exist, by storing the hashes separately from the values.

// 0x7cdd44b as 32 bit prime, as backwards it is almost prime (except for leading 0s), and backwards
//	is the largest prime (that is also a debruijn B(2, 32) sequence)
//	except... we need a 64 bit prime... Or... actually, if we only use the last bits of the hash anyway,
//	then 32 bits is fine. As long as the input hash is already nicely distributed.

#include <unordered_map>
#include <cstdio>
#include <chrono>

#include "mersenne-twister.h"

#include "simple_table.h"


void randomBytes(unsigned char* key, int size, unsigned long long seed) {
	mersenne_seed((unsigned long)seed);
	for (int i = 0; i < size; i++) {
		unsigned int v = mersenne_rand_u32();
		// Eh... this might be a bit biased. But I don't think it impacts the security of this. Also... this probably won't be called anyway.
		key[i] = (char)v;
	}
}


typedef struct {
	int ipSrc;
	int ipDst;
	short portSrc;
	short portDst;
} Entry;

int hashEntry(Entry* entry) {
	unsigned long long curHash = 0;
	unsigned char* bytes = (unsigned char*)entry;
	for (int i = 0; i < sizeof(Entry); i++) {
		unsigned char b = bytes[i];
		curHash = (curHash * 107) ^ b;
	}

	return (int)((curHash >> 32) ^ (curHash));
}



void runEntriesTest() {
	Entry entries[1];
	Entry entries2[1];
	int entry_count = sizeof(entries) / sizeof(Entry);

	randomBytes((unsigned char*)entries, sizeof(entries), 0x7cdd44b7cdd44b);
	randomBytes((unsigned char*)entries2, sizeof(entries2), 0x2324);

#define use_std 1
	
#ifdef use_std
	std::unordered_map<int, Entry> items;
#else
#endif
	for (int i = 0; i < entry_count; i++) {
		items.emplace(hashEntry(&entries[i]), entries[i]);
	}

	int entry3_count = 1024 * 1024;
	Entry* entries3 = (Entry*)malloc(sizeof(Entry) * entry3_count);
	Entry notInSet = entries2[0];
	for (int i = 0; i < entry3_count; i++) {
		entries3[i] = notInSet;
	}


	int totalCount = 1024 * 1024 * 10;

	int hash = 0;

	auto start = std::chrono::high_resolution_clock::now();
	for (int y = 0; y < totalCount / entry3_count; y++) {
		for (int i = 0; i < entry3_count; i++) {
			if (items.find(hashEntry(&entries3[i])) != items.end()) {
				printf("found at %d\n", i);
			}
		}
	}
	auto end = std::chrono::high_resolution_clock::now();

	std::chrono::duration<double> time = end - start;

	std::chrono::seconds s{ 1 };

	printf("hash %d\n", hash);

	printf("Seconds %fs, Per second %dm\n", time.count(), (int)(totalCount / time.count() / 1000 / 1000));
}

#include <Windows.h>

void runInterlockedTest() {
	int totalSize = 1024 * 1024;
	long* data = (long*)malloc(totalSize);
	randomBytes((unsigned char*)data, totalSize, 0x7cdd44b7cdd44b);

	int count = totalSize / sizeof(long);
	int loopCount = 1024;

	int hash = 0;

	int version = 0;

	auto start = std::chrono::high_resolution_clock::now();
	for (int y = 0; y < loopCount; y++) {
		for (int i = 1; i < count; i++) {
			/*
			InterlockedCompareExchange(
				&data[i],
				data[0],
				data[0]
			);
			//*/
			if (data[i] == data[0]) {
				if (version + 1 == version) {
					version++;
				}
				printf("found at %d\n", i);
			}
		}
	}
	auto end = std::chrono::high_resolution_clock::now();

	std::chrono::duration<double> time = end - start;

	std::chrono::seconds s{ 1 };



	printf("Seconds %fs, %fns per for %d\n",
		time.count(),
		((double)time.count() * 1000 * 1000 * 1000 / (count * loopCount)),
		count * loopCount
	);
}

#include <windows.h>
void SpawnThread(
	HANDLE volatile* pThread,
	void* context,
	DWORD(*main)(void*)
) {
	*pThread = CreateThread(
		0,
		0,
		main,
		context,
		0,
		0
	);
}




#include "TransactionQueue.h"
#include "TransactionQueueHelpers.h"

//#define VALUE_COUNT 25
//#define THREAD_COUNT 10
//uint64_t count = 1000 * 10;

//#define VALUE_COUNT 1
//#define THREAD_COUNT 1
//uint64_t count = 10;

#define VALUE_COUNT 10
#define THREAD_COUNT 1
uint64_t count = 1000 * 100;

uint64_t writesPerTransaction = VALUE_COUNT;
TransactionQueue transactions = { 0 };

#pragma pack(push, 1)
typedef struct {
	int values[VALUE_COUNT];
} BaseStruct;
#pragma pack(pop)


#pragma pack(push, 1)
union __declspec(align(16)) TypeName {
	AtomicUnit units[sizeof(BaseStruct) / BYTES_PER_TRANSACTION];
	// The base struct is exposed to make it easy to get offsets
	BaseStruct v;
};
#pragma pack(pop)

TypeName InstanceName = { 0 };


//todonext
// Make some nice macros to use with TransactionQueue to allow
//	1) Wrapping a static struct to make it union with memory 4x larger than it
//	2) Apply gets, which will get the data from the fragmented pattern, back into regular values, taking the offset
//		from the struct to know the position
//	3) Apply sets, spread the values out, AND THEN CALL insertWrite (or maybe just return TransactionChange)
//	4) AND THEN change the union so it also adds a dynamic allocation table? And understands that some writes will
//		need to go to that table?
//		- Hmm... it does kind of need to know about the table, or else it won't know how to generate the dataIndex for
//			creating TransactionChanges. But... is this too much? Hmm... maybe it can just handle everything?,
//			and even provide the underlying applyChange function, and it can even automatically insert
//			index moves, and deal with partial moves, AND it can even allocate new memory... but it will need
//			some help doing that, as it won't really know how 'full' any allocation is, as that depends on
//			if its a compressed list, a hash table, etc...
//TransactionQueue

void inside() {
	for (int i = 0; i < count; i++) {
		int result = TransactionQueue_ApplyWrite(
			&transactions,
			&InstanceName,
			[](void* writeContext, auto c, auto insert, auto finish) {
				TypeName* v = (TypeName*)writeContext;
				for (int i = 0; i < writesPerTransaction; i++) {
					int newValue = Get_int32_t(v->units, v->v.values + i) + 1;
					int result = Set_int32_t(c, insert, v->units, v->v.values + i, newValue);

					/*
					//int newValue = v->units[i].value + 1;
					TransactionChange change;
					TransactionChange_set_dataIndex(&change, i);
					change.newValue = newValue;
					int result = insert(c, change);
					//*/

					if (result != 0) return result;
				}
				return finish(c);
			},
			&InstanceName,
			ApplyStructChange
		);
		if (result != 0) {
			printf("ApplyWrite failure, %d\n", result);
		}
	}
};

void writeInfo(const char* name, CallTimes* times) {
	printf("%s %llu calls, %f per call, %llu untimed\n",
		name,
		times->timedCalls,
		(double)times->time / times->timedCalls,
		times->untimedCalls
	);
}

void runTransactionTest() {

	auto start = std::chrono::high_resolution_clock::now();

	HANDLE threads[THREAD_COUNT];
	for (int i = 0; i < THREAD_COUNT; i++) {
		SpawnThread(
			&threads[i],
			nullptr,
			[](auto context) -> DWORD {
				try {
					inside();
				}
				catch (...) {
					printf("error\n");
				}
				return 0;
			}
		);
	}

	// WaitForMultipleObjects is garbage as it has a max of 64, so... don't use it.
	for (int i = 0; i < THREAD_COUNT; i++) {
		WaitForSingleObject(threads[i], INFINITE);
	}

	TypeName* v = (TypeName*)&InstanceName;
	for (int i = 0; i < VALUE_COUNT; i++) {
		int value = Get_int32_t(v->units, &v->v.values[i]);
		if (value != THREAD_COUNT * count) {
			printf("value=%llu\n", (uint64_t)value);
		}
	}


	
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> time = end - start;
	std::chrono::seconds s{ 1 };

	uint64_t totalCount = count * THREAD_COUNT * writesPerTransaction;

	printf("Seconds %fs, %fns per for %llu\n",
		time.count(),
		((double)time.count() * 1000 * 1000 * 1000 / totalCount),
		totalCount
	);

	printf("retryLoopCount=%llu\n", transactions.retryLoopCount);

	printf("retries per apply=%f\n", (double)transactions.retryLoopCount / transactions.applyCallCount);
	printf("retries per dequeue=%f\n", (double)transactions.retryLoopCount / transactions.dequeueCount);
	if (transactions.hardFails > 0) {
		printf("hardFails=%llu\n", transactions.hardFails);
	}

	
	CancellableLock& d = transactions.writeLock;
	printf("work loop count=%llu\n", d.workFinishedCount);
	printf("work loop count per retry=%f\n", (double)d.workFinishedCount / transactions.retryLoopCount);
	printf("work per loop\t\t=%f\n", (double)d.workTime / d.workFinishedCount);
	printf("blockedTime per loop\t=%f\n", (double)d.blockedTime / d.workFinishedCount);
	printf("lock failures per loop\t=%f\n", (double)d.lockFailures / d.workFinishedCount);
	printf("unneeded cancels per loop=%f\n", (double)d.unneededCancels / d.workFinishedCount);
	printf("polls per loop=%f\n", (double)d.waitPollCount / d.workFinishedCount);
	printf("ticks to wait=%llu\n", d.ticksToWait);
}

extern "C" {
	void OnError(int code) {
		printf("Error %d\n");
	}
}

int matches2 = 0;
int dataValue;
void runReadTest() {
	int readCount = 1024 * 1024 * 10;

	unsigned char* data = (unsigned char*)malloc(readCount);
	randomBytes((unsigned char*)data, readCount, 0x7cdd44b7cdd44b);

	uint64_t totalCount = readCount * VALUE_COUNT;


	int result = TransactionQueue_ApplyWrite(
		&transactions,
		&InstanceName,
		[](void* writeContext, auto c, auto insert, auto finish) {
			TypeName* v = (TypeName*)writeContext;
			for (int i = 0; i < VALUE_COUNT; i++) {
				int result = Set_int32_t(c, insert, v->units, &v->v.values[i], i);
				if (result != 0) return result;
			}

			return finish(c);
		},
		&InstanceName,
		ApplyStructChange
	);
	if (result != 0) {
		printf("ApplyWrite failure, %d\n", result);
	}


	int values[VALUE_COUNT];
	for (int i = 0; i < VALUE_COUNT; i++) {
		values[i] = i;
	}


	auto start = std::chrono::high_resolution_clock::now();

	int matches1 = 0;
	//*
	for (int x = 0; x < readCount; x++) {
		int dataValue = data[x];
		//BaseStruct* v = (BaseStruct*)&InstanceName;
		for (int i = 0; i < VALUE_COUNT; i++) {
			//int value = Get_Int32(v, &v->values[i]);
			int value = values[i];
			if (value == dataValue) {
				matches1++;
			}
		}
	}
	//*/

	
	/*
	for (int x = 0; x < readCount; x++) {
		dataValue = data[x];
		TransactionQueue_RunGetter(
			&transactions,
			&InstanceName,
			[](void* getContext) {
				BaseStruct* v = (BaseStruct*)getContext;
				for (int i = 0; i < VALUE_COUNT; i++) {
					int value = Get_Int32(v, &v->values[i]);
					if (value == dataValue) {
						matches2++;
					}
				}
				return 0;
			},
			&InstanceName,
			ApplyStructChange
		);
	}
	*/

	printf("match1 %d, match2 %d\n", matches1, matches2);


	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> time = end - start;
	std::chrono::seconds s{ 1 };


	printf("Seconds %fs, %fns per for %llu\n",
		time.count(),
		((double)time.count() * 1000 * 1000 * 1000 / totalCount),
		totalCount
	);
}


void allocTest() {
	uint64_t count = 1024 * 1024 * 10;

	auto start = std::chrono::high_resolution_clock::now();

	uint64_t hash = 0;

	for (int i = 0; i < count; i++) {
		void* memory = malloc(4);
		hash += (uint64_t)memory;
		free(memory);
	}



	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> time = end - start;
	std::chrono::seconds s{ 1 };

	printf("hash %llu\n", hash);


	printf("Seconds %fs, %fns per for %llu\n",
		time.count(),
		((double)time.count() * 1000 * 1000 * 1000 / count),
		count
	);
}

#include "ReferenceCounter.h"
void runRefTest() {
//#define NATIVE_ALLOC

	uint64_t count = 1024 * 1024;

	uint64_t* pointers = (uint64_t*)malloc(count * sizeof(uint64_t));


	auto start = std::chrono::high_resolution_clock::now();

	uint64_t hash = 0;

	// Also code rolling allocations too, where we always keep a minimum around,
	//	but then allocate and unallocate within that, always the oldest one

	for (int i = 0; i < count; i++) {
		if (i == count / 2) {
			int l = 0;
		}
		void* p;
#ifdef NATIVE_ALLOC
		p = malloc(sizeof(uint64_t));
		*(uint64_t*)p = (count - i);
		pointers[i] = (uint64_t)p;
#else
		uint64_t smallPointer = AllocateAsSmallPointer(sizeof(uint64_t), &p);
		pointers[i] = smallPointer;
#endif
		*(uint64_t*)p = (count - i);
		hash += pointers[i];
	}

	for (int i = 0; i < count; i++) {
		void* p;
#ifdef NATIVE_ALLOC
		p = (void*)pointers[i];
#else
		uint64_t smallPointer = pointers[i];
		p = SafeReferenceSmallPointer64(&smallPointer);
		DereferenceSmallPointer(smallPointer);
#endif
		hash += *(uint64_t*)p;
#ifdef NATIVE_ALLOC
		free(p);
#else
		DereferenceSmallPointer(smallPointer);
#endif
	}

	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> time = end - start;
	std::chrono::seconds s{ 1 };

	printf("hash %llu\n", hash);

	printf("Search efficiency %f\n", (double)debug_getPointerTable()->searchStarts / debug_getPointerTable()->searchIterations);


	printf("Seconds %fs, %fns per for %llu\n",
		time.count(),
		((double)time.count() * 1000 * 1000 * 1000 / count),
		count
	);
}

void runRefTest2() {
	#define NATIVE_ALLOC

	uint64_t count = 1;
	uint64_t bytes = 1024 * 1024;

	uint64_t* pointers = (uint64_t*)malloc(bytes * sizeof(uint64_t));


	auto start = std::chrono::high_resolution_clock::now();

	uint64_t hash = 0;

	// Also code rolling allocations too, where we always keep a minimum around,
	//	but then allocate and unallocate within that, always the oldest one

	for (int k = 0; k < bytes; k++) {
		void* p;
#ifdef NATIVE_ALLOC
		p = malloc(sizeof(uint64_t));
		*(uint64_t*)p = (count - k);
		pointers[k] = (uint64_t)p;
#else
		uint64_t smallPointer = AllocateAsSmallPointer(sizeof(uint64_t), &p);
		pointers[k] = smallPointer;
#endif
		*(uint64_t*)p = (count - k);
		hash += pointers[k];
	}

	/*
	for (int k = 0; k < count * bytes; k++) {
		int i = k % bytes;
		
#ifdef NATIVE_ALLOC
		free((void*)pointers[i]);
		pointers[i] = (uint64_t)malloc(sizeof(uint64_t));
#else
		DereferenceSmallPointer(pointers[i]);
		void* p;
		pointers[i] = AllocateAsSmallPointer(sizeof(uint64_t), &p);
#endif
		hash += pointers[i];
	}
	//*/

	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> time = end - start;
	std::chrono::seconds s{ 1 };

	printf("hash %llu\n", hash);

	uint64_t totalCount = count * bytes;
	printf("Search efficiency %f\n", (double)debug_getPointerTable()->searchIterations / debug_getPointerTable()->searchStarts);
	printf("malloc calls %f\n", (double)debug_getPointerTable()->mallocCalls / totalCount);

	
	printf("Seconds %fs, %fns per for %llu\n",
		time.count(),
		((double)time.count() * 1000 * 1000 * 1000 / totalCount),
		totalCount
	);
}

int main() {
	try {
		runRefTest2();
	}
	catch (...) {
		printf("error main\n");
	}

	//runReadTest();

	return 0;
}