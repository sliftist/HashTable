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

#include <immintrin.h>



void randomBytes(unsigned char* key, int size, unsigned long long seed) {
	mersenne_seed((unsigned long)seed);
	for (int i = 0; i < size; i++) {
		unsigned int v = mersenne_rand_u32();
		// Eh... this might be a bit biased. But I don't think it impacts the security of this. Also... this probably won't be called anyway.
		key[i] = (char)v;
	}
}

void randomBytesSecure(unsigned char* key, int size) {
	for (int i = 0; i < size;) {
		uint64_t value = 0;
		if (_rdrand64_step(&value) == 0) {
			throw "impossible";
		}
		
		if (size - i < 8) {
			key[i] = (char)value;
			i++;
		}
		else {
			*(uint64_t*)(key + i) = value;
			i += 8;
		}
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
	void OnErrorInner(int code, const char* name, unsigned long long line) {
		printf("Error %d, %s:%llu\n", code, name, line);
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

#include "AtomicHashTable.h"


uint64_t invertBits(uint64_t value) {
	uint64_t output = 0;
	uint64_t mask = 1;
	for (int bit = 0; bit < 63; bit++) {
		output = output | ((value & mask) >> bit << (63 - bit));
		mask = mask << 1;
	}
	return output;
}
uint64_t getHash(uint64_t a, uint64_t b) {
	return invertBits(a + b);
}


typedef struct {
	uint64_t a;
	uint64_t b;
	uint64_t c;
} Item;
void testAdd(AtomicHashTable& table, uint64_t a, uint64_t b, uint64_t c) {
	Item* item;
	uint64_t itemSmallPointer = AllocateAsSmallPointer(sizeof(Item), (void**)&item);
	item->a = a;
	item->b = b;
	item->c = c;
	uint64_t hash = getHash(a, b);
	ErrorTop(AtomicHashTable_insert(&table, hash, itemSmallPointer));
}

uint64_t testRemove(AtomicHashTable& table, Item* item) {
	uint64_t hash = getHash(item->a, item->b);
	typedef struct {
		Item item;
		uint64_t count;
	} Context;
	Context context = { 0 };
	context.item = *item;
	int result = (AtomicHashTable_remove(&table, hash, &context, [](void* callbackContextVoid, void* valueVoid){
		Context* context = (Context*)callbackContextVoid;
		Item* item = &context->item;
		Item* other = (Item*)valueVoid;
		int shouldRemove = (int)(item->a == other->a && item->b == other->b);
		if (shouldRemove) {
			context->count++;
		}
		return shouldRemove;
	}));
	ErrorTop(result);
	return context.count;
}
Item testGetSome(AtomicHashTable& table, Item* item) {
	uint64_t hash = getHash(item->a, item->b);
	typedef struct {
		Item itemIn;
		Item itemOut;
	} Context;
	Context context = { 0 };
	context.itemIn = *item;

	ErrorTop(AtomicHashTable_find(&table, hash, &context, [](void* contextVoid, void* valueAny) {
		Context* context = (Context*)contextVoid;
		Item* value = (Item*)valueAny;
		if (context->itemIn.a == value->a && context->itemIn.b == value->b) {
			context->itemOut = *value;
		}
	}));
	return context.itemOut;
}
uint64_t testGetCount(AtomicHashTable& table, uint64_t a, uint64_t b) {

	typedef struct {
		Item item;
		uint64_t count;
	} Context;
	Context context = { 0 };
	context.item.a = a;
	context.item.b = b;

	uint64_t hash = getHash(a, b);
	ErrorTop(AtomicHashTable_find(&table, hash, &context, [](void* contextAny, void* value){
		Context* context = (Context*)contextAny;
		Item* item = (Item*)value;
		if (context->item.a == item->a && context->item.b == item->b) {
			context->count++;
		}
	}));
	return context.count;
}

void AssertEqual(uint64_t correct, uint64_t test) {
	if(correct != test) {
		printf("Error. Correct %llu was %llu\n", correct, test);
	}
}

void runAtomicHashTableTestInner(AtomicHashTable& table) {
	testAdd(table, 0, 1, 2);
	{
		Item item = { 0 };
		item.a = 0;
		item.b = 1;
		item.c = 2;
		uint64_t v = testGetSome(table, &item).c;
		AssertEqual(2, v);
	}
	{
		Item item = { 0 };
		item.a = 0;
		item.b = 1;
		item.c = 2;
		uint64_t v = testGetSome(table, &item).c;
		AssertEqual(2, v);
	}
}



typedef struct {
	uint64_t fillCount;
	PointersSnapshot* snapshot;
	AtomicHashTable* table;
} TableSnapshot;

TableSnapshot GetSnapshot(AtomicHashTable& table) {
	return { DebugAtomicHashTable_properties(&table).currentFillCount, Debug_GetPointersSnapshot(), &table };
}

int CompareSnapshot(TableSnapshot info) {
	
	AssertEqual(info.fillCount, DebugAtomicHashTable_properties(info.table).currentFillCount);

	PointersSnapshotDelta delta = Debug_ComparePointersSnapshot(info.snapshot);
	if(delta.changeType) {
		if(delta.changeType == 1) {
			printf("Removed pointer size %llu allocated at %s:%llu, value %llu\n", delta.entry.size, delta.entry.fileName, delta.entry.line, delta.entry.smallPointerNumber);
		} else if(delta.changeType == 2) {
			printf("Added pointer size %llu allocated at %s:%llu, value %llu\n", delta.entry.size, delta.entry.fileName, delta.entry.line, delta.entry.smallPointerNumber);
		} else if(delta.changeType == 3) {
			printf("Changed ref count of pointer size %llu, by %llu, allocated at %s:%llu, value %llu\n", delta.entry.size, delta.refCountDelta, delta.entry.fileName, delta.entry.line, delta.entry.smallPointerNumber);
		}
	}
	return delta.changeType;
}

void testHashLeaksRefs() {
	AtomicHashTable table = { 0 };

	auto zeroState = GetSnapshot(table);
	
	runAtomicHashTableTestInner(table);
	auto afterRun = GetSnapshot(table);
	AtomicHashTable_dtor(&table);

	CompareSnapshot(zeroState);

	runAtomicHashTableTestInner(table);
	CompareSnapshot(afterRun);
}

void testHashChurnVar(int variation) {
	uint64_t stride = 10;
	uint64_t totalCount = 1000;

	//stride = 1;
	//totalCount = 100;

	Item* items = (Item*)malloc(totalCount * sizeof(Item));
	if (variation == 0) {
		memset(items, 1, totalCount * sizeof(Item));
	}
	if (variation == 1) {
		randomBytes((unsigned char*)items, (int)(totalCount * sizeof(Item)), 0x7cdd44b);
	}

	AtomicHashTable table = { 0 };
	auto zeroState = GetSnapshot(table);

	TableSnapshot firstInsert = { 0 };

	for (uint64_t i = 0; i < totalCount; i += stride) {
		for (uint64_t j = 0; j < stride; j++) {
			Item* item = &items[i + j];
			testAdd(table, item->a, item->b, item->c);
		}
		if(!firstInsert.snapshot) {
			firstInsert = GetSnapshot(table);
		}
		else {
			//printf("compare\n");
			if (CompareSnapshot(firstInsert)) {
				printf("\tat i=%llu\n", i);
				break;
			}
		}
		
		for (int j = 0; j < stride; j++) {
			Item* item = &items[i + j];
			//AssertEqual(testGetSome(table, item).c, item->c);
		}
		for (int j = 0; j < stride; j++) {
			Item* item = &items[i + j];
			testRemove(table, item);
		}
	}

	AtomicHashTable_dtor(&table);
	CompareSnapshot(zeroState);
}
void testHashChurn() {
	testHashChurnVar(0);
	testHashChurnVar(1);
}



#include "Timing.h"
#include "TimingDebug.h"

void testSizingVar(int variation) {
	uint64_t factor = 1;
	uint64_t itemCount;
	
	if (variation == 0) {
		itemCount = 1000;
	}
	else if(variation == 1) {
		itemCount = 30000;
		factor = 30000;
		itemCount = itemCount / factor;
	}
	else if(variation == 2) {
		itemCount = (1ll << 26);
	}
	else {
		itemCount = (1ll << 17);
	}


	AtomicHashTable table = { 0 };

	TableSnapshot zeroState;
	if (variation == 0 || variation == 1 || variation == 3) {
		zeroState = GetSnapshot(table);
	}

	if (variation == 2 || variation == 3) {
		printf("insert + 2 gets + remove + dtor timing\n");
		Timing_StartRoot(&rootTimer);
	}

	for (uint64_t i = 0; i < itemCount; i++) {
		testAdd(table, i, i, i);

		if (variation == 2) {
			if (i % (itemCount / 100) == 0) {
				auto props = DebugAtomicHashTable_properties(&table);
				uint64_t count = props.currentFillCount;
				uint64_t maxCount = 1ll << (props.currentAllocationLog - 1);
				printf("Add at %f%% %llu/%llu\n", (double)(i + 1) / itemCount * 100, count, maxCount);
			}
		}

		if (variation == 0) {
			for (uint64_t j = 0; j <= i; j++) {
				uint64_t count = testGetCount(table, j, j);
				AssertEqual(count, 1);
			}
		}
	}

	if (variation == 0 || variation == 2 || variation == 3) {
		for (uint64_t j = 0; j < itemCount; j++) {
			if (variation == 2) {
				if (j % (itemCount / 100) == 0) {
					auto props = DebugAtomicHashTable_properties(&table);
					uint64_t count = props.currentFillCount;
					uint64_t maxCount = 1ll << (props.currentAllocationLog - 1);
					printf("Check at %f%% %llu/%llu\n", (double)(j + 1) / itemCount * 100, count, maxCount);
				}
			}

			{
				uint64_t count = testGetCount(table, j + itemCount, j + itemCount);
				AssertEqual(count, 0);
			}
			{
				uint64_t count = testGetCount(table, j, j);
				AssertEqual(count, 1);
			}
		}
	}
	else {
		printf("no matches:\n");
		Timing_StartRoot(&rootTimer);
		for (uint64_t k = 0; k < factor; k++) {
			for (uint64_t j = 0; j < itemCount; j++) {
				TimeBlock(gets,
					uint64_t count = testGetCount(table, j + itemCount, j + itemCount);
				AssertEqual(count, 0);
				);
			}
		}
		Timing_EndRootPrint(&rootTimer, itemCount * factor);

		printf("all matches:\n");
		Timing_StartRoot(&rootTimer);
		for (uint64_t k = 0; k < factor; k++) {
			for (uint64_t j = 0; j < itemCount; j++) {
				TimeBlock(gets,
					uint64_t count = testGetCount(table, j, j);
				AssertEqual(count, 1);
				);
			}
		}
		Timing_EndRootPrint(&rootTimer, itemCount * factor);
	}


	for (uint64_t i = 0; i < itemCount; i++) {
		if (variation == 2) {
			if (i % (itemCount / 100) == 0) {
				auto props = DebugAtomicHashTable_properties(&table);
				uint64_t count = props.currentFillCount;
				uint64_t maxCount = 1ll << (props.currentAllocationLog - 1);
				printf("Remove at %f%% %llu/%llu\n", (double)(i + 1) / itemCount * 100, count, maxCount);
			}
		}

		Item item = { 0 };
		item.a = i;
		item.b = i;
		item.c = i;
		testRemove(table, &item);

		if (variation == 0) {
			for (uint64_t j = 0; j <= i; j++) {
				AssertEqual(testGetCount(table, j, j), 0);
			}
			for (uint64_t j = i + 1; j < itemCount; j++) {
				AssertEqual(testGetCount(table, j, j), 1);
			}
		}
	}

	AtomicHashTable_dtor(&table);
	if (variation == 0 || variation == 1 || variation == 3) {
		CompareSnapshot(zeroState);
	}

	if (variation == 2 || variation == 3) {
		Timing_EndRootPrint(&rootTimer, itemCount * factor);
	}
}
void testSizing() {
	testSizingVar(0);
	testSizingVar(1);
	//testSizingVar(2);
	testSizingVar(3);
}

#include <vector>
void testHashChurn2Var(int variation) {

	uint64_t itemCount = 1000;
	uint64_t iterationCount = itemCount * 10;


	int16_t* randomChoices = (int16_t*)malloc(iterationCount * sizeof(int16_t));
	int64_t* randomIndexes = (int64_t*)malloc(iterationCount * sizeof(int64_t));
	Item* items = (Item*)malloc(itemCount * sizeof(Item));


	if (variation == 0) {
		randomBytes((unsigned char*)randomChoices, (int)(iterationCount * sizeof(int16_t)), 0x7cdd44b);
		randomBytes((unsigned char*)randomIndexes, (int)(iterationCount * sizeof(int64_t)), 0x7cdd44b);

		for (uint64_t i = 0; i < itemCount; i++) {
			items[i].a = i + 1;
			items[i].b = i * 2;
			items[i].c = i;
		}
	}
	else {
		randomBytesSecure((unsigned char*)randomChoices, (int)(iterationCount * sizeof(int16_t)));
		randomBytesSecure((unsigned char*)randomIndexes, (int)(iterationCount * sizeof(int64_t)));
		randomBytesSecure((unsigned char*)items, (int)(itemCount * sizeof(Item)));
	}


	AtomicHashTable table = { 0 };

	// Cause an allocation, to get the "empty, but has had data before" state (see the final zeroSnapshot check comment for why this matters)
	testAdd(table, 1, 1, 1);
	Item item = { 0 };
	item.a = 1;
	item.b = 1;
	item.c = 1;
	testRemove(table, &item);

	auto zeroSnapshot = GetSnapshot(table);


	std::vector<Item> itemsNotAdded(items, items + itemCount);
	std::vector<Item> itemsAdded;

	int16_t decision = 0;

	for(uint64_t i = 0; i < iterationCount; i++) {
		if (i == 5486) {
			int here = 0;
		}

		int64_t index = randomIndexes[i];
		if((decision >= 0 || itemsAdded.size() == 0) && itemsNotAdded.size() > 0) {
			index = index % itemsNotAdded.size();
			Item item = itemsNotAdded.at(index);
			if (item.c == 981) {
				int here = 0;
			}
			testAdd(table, item.a, item.b, item.c);
			AssertEqual(testGetCount(table, item.a, item.b), 1);
			AssertEqual(testGetSome(table, &item).c, item.c);
			itemsNotAdded.erase(itemsNotAdded.begin() + index);
			itemsAdded.push_back(item);
		} else {
			index = index % itemsAdded.size();
			Item item = itemsAdded.at(index);
			if (item.c == 981) {
				int here = 0;
			}
			uint64_t count = testRemove(table, &item);

			AssertEqual(count, 1);

			itemsAdded.erase(itemsAdded.begin() + index);
			itemsNotAdded.push_back(item);
		}
		decision += randomChoices[i] / 256;

		//if (i == 5486)
		{
			for (uint64_t j = 0; j < itemsAdded.size(); j++) {
				Item item = itemsAdded.at(j);
				if (j == 174) {
					int here = 0;
				}
				AssertEqual(testGetCount(table, item.a, item.b), 1);
			}
			for (uint64_t j = 0; j < itemsNotAdded.size(); j++) {
				Item item = itemsNotAdded.at(j);
				AssertEqual(testGetCount(table, item.a, item.b), 0);
			}
		}
	}

	for(uint64_t i = 0; i < itemsAdded.size(); i++) {
		Item item = itemsAdded.at(i);
		AssertEqual(testGetCount(table, item.a, item.b), 1);
	}

	for(uint64_t i = 0; i < itemsAdded.size(); i++) {
		if (i + 1 == itemsAdded.size()) {
			int here = 0;
		}
		Item item = itemsAdded.at(i);
		uint64_t count = testRemove(table, &item);
		AssertEqual(count, 1);
	}

	// So, the snapshot should be equal, with the only allocation the initial minimal allocation (this verifies that we deallocate down to
	//	the minimum allocation... which may not be what we want, so if this starts failing... maybe just change this test...)
	CompareSnapshot(zeroSnapshot);
}

void testHashChurn2() {
	testHashChurn2Var(0);
	testHashChurn2Var(1);
}




void runAtomicHashTableTest() {
	testHashLeaksRefs();

	testHashChurn();
	testSizing();
	testHashChurn2();

	todonext
	// I think this works... Now... get it building in the kernel, then running for a toy example in the kernel, and then...
	//	start using it to dynamically measure network traffic, and then dynamically filter/control network traffic.
}

int main() {
	try {
		runAtomicHashTableTest();
	}
	catch (...) {
		printf("error main\n");
	}

	//runReadTest();

	return 0;
}