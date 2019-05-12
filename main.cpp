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

#include <immintrin.h>



void randomBytes(unsigned char* key, int size, unsigned long long seed) {
	MTState state = { 0 };
	mersenne_seed(&state, (unsigned long)seed);
	for (int i = 0; i < size; i++) {
		unsigned int v = mersenne_rand_u32(&state);
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



extern "C" {
	void OnErrorInner(int code, const char* name, unsigned long long line) {
		printf("Error %d, %s:%llu\n", code, name, line);
	}
}




#include "AtomicHashTable2.h"


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
	uint64_t hash = invertBits(a + b);
	if (hash == 0) {
		hash = 1;
	}
	return hash;
}


typedef struct {
	uint64_t a;
	uint64_t b;
	uint64_t c;
} Item;

// TODO: Oh, pass Item** in, so we will actually be simulating dynamic allocation and memory management.
void deleteItem(void* itemVoid) {
	Item* item = (Item*)itemVoid;
	//delete item;
}

#include <functional>

typedef void(*CallbackFunction)(void* context, void* value);
template <typename T, typename Result>
class FunctionWrapper {
private:
	const std::function<Result(T)> fnc;
public:
	FunctionWrapper(const std::function<Result(T)> fnc): fnc(fnc) { }

	void* getContext() {
		return (void*)this;
	}

	static Result callbackFnc(void* context, void* value) {
		auto wrapper = (FunctionWrapper<T, Result>*)context;
		return wrapper->fnc((T)value);
	}
};



void testAdd2(AtomicHashTable2& table, uint64_t a, uint64_t b, uint64_t c) {
	Item item;
	item.a = a;
	item.b = b;
	item.c = c;
	uint64_t hash = getHash(a, b);

	ErrorTop(AtomicHashTable2_insert(&table, hash, &item));
}


uint64_t testRemove2(AtomicHashTable2& table, Item& item) {
	uint64_t hash = getHash(item.a, item.b);
	uint64_t count = 0;

	FunctionWrapper<Item*, int> removeCallback([&](Item* other) {
		int shouldRemove = (int)(item.a == other->a && item.b == other->b && item.c == other->c);
		if (shouldRemove) {
			count++;
		}
		return shouldRemove;
	});
	
	int result = AtomicHashTable2_remove(&table, hash, removeCallback.getContext(), removeCallback.callbackFnc);
	ErrorTop(result);
	return count;
}
Item testGetSome2(AtomicHashTable2& table, Item& item) {
	uint64_t hash = getHash(item.a, item.b);
	Item someItem = { 0 };

	FunctionWrapper<Item*, void> findCallback([&](Item* other) {
		if(item.a == other->a && item.b == other->b && item.c == other->c) {
			someItem = *other;
		}
	});

	int result = AtomicHashTable2_find(&table, hash, findCallback.getContext(), findCallback.callbackFnc);
	ErrorTop(result);
	return someItem;
}
uint64_t testGetCount2(AtomicHashTable2& table, uint64_t a, uint64_t b) {
	typedef struct {
		Item item;
		uint64_t count;
	} Context;
	Context context = { 0 };
	context.item.a = a;
	context.item.b = b;

	uint64_t hash = getHash(a, b);

	int result = AtomicHashTable2_find(&table, hash, &context, [](void* contextAny, void* value) {
		Context* context = (Context*)contextAny;
		Item* item = (Item*)value;
		if (context->item.a == item->a && context->item.b == item->b) {
			context->count++;
		}
	});
	ErrorTop(result);
	return context.count;
}


//#define AssertEqual(correct, test) AssertEqualInner(correct, test, __FILE__, __LINE__)
#define AssertEqual(correct, test) !AssertEqualInner(correct, test, __FILE__, __LINE__) && !AssertEqualInner(correct, test, "repeat fail", __LINE__)

bool AssertEqualInner(uint64_t test, uint64_t correct, const char* file, unsigned long long line) {
	if(correct != test) {
		printf("Error. Was %llu correct is %llu at %s:%llu\n", test, correct, file, line);
		breakpoint();
		return false;
	}
	return true;
}





#include "Timing.h"
#include "TimingDebug.h"


typedef struct {
	AtomicHashTable2* table;
	int variation;
	int threadIndex;
	HANDLE thread;
} TableMultiThreadsContext;
void testTableMultiThreads(
	int count,
	int variationStart,
	int variationCount,
	DWORD(*runThread)(TableMultiThreadsContext*)
) {
	for(int v = variationStart; v < variationStart + variationCount; v++) {
		AtomicHashTable2 table = AtomicHashTableDefault(sizeof(Item), deleteItem);

		TableMultiThreadsContext* threads = new TableMultiThreadsContext[count];

		printf("Starting %d threads\n", count);

		memset(threads, 0, sizeof(HANDLE) * count);
		for (int i = 0; i < count; i++) {
			TableMultiThreadsContext* context = &threads[i];
			context->table = &table;
			context->variation = v;
			context->threadIndex = i;
			SpawnThread(
				&context->thread,
				context,
				(DWORD(*)(void*))runThread
			);
		}

		// WaitForMultipleObjects is garbage as it has a max of 64, so... don't use it.
		for (int i = 0; i < count; i++) {
			WaitForSingleObject(threads[i].thread, INFINITE);
			printf("Finished thread %d\n", i);
		}
	}
}

uint64_t groupSum;
uint64_t groupCount;
uint64_t groupMax;
uint64_t tickSum;
uint64_t tickCount;
uint64_t tickMax;
void tickGroupStart() {
	groupSum = 0;
	groupCount = 0;
	groupMax = 0;
}

uint64_t tickStartTime = 0;
void tickStart() {
	tickStartTime = GetTime();
}
void tickEnd() {
	uint64_t tickEndTime = GetTime();
	uint64_t curTime = tickEndTime - tickStartTime;
	groupSum += curTime;
	groupCount++;
	groupMax = max(groupMax, curTime);

	if (curTime > 108915300) {
		//breakpoint();
	}

	tickSum += curTime;
	tickCount++;
	tickMax = max(tickMax, curTime);
}
void printPercentTick(const char* tickName, AtomicHashTable2& table, double fraction) {
	uint64_t count = DebugAtomicHashTable2_reservedSize(&table);
	uint64_t maxCount = DebugAtomicHashTable2_allocationSize(&table);
	printf("%s at %f%% %llu/%llu, %llu time %f%% of all, %llu average time, %f%% average of all, max %fX average, %f%% tick max of worst max, worst %f%%\n",
		tickName, fraction * 100, count, maxCount,
		tickSum,
		(double)tickSum / groupSum * 100,
		tickSum / tickCount,
		(double)(tickSum / tickCount) / (groupSum / groupCount) * 100,
		(double)tickMax / (tickSum / tickCount),
		(double)tickMax / groupMax * 100,
		(double)tickMax / tickSum * 100
	);
	tickSum = 0;
	tickCount = 0;
	tickMax = 0;
}


void testSizingVarInner(AtomicHashTable2& table, int variation, int threadIndex) {
	
	//todonext
	// Do all single threaded tests, and then do multithreaded tests, in debug and release...

	uint64_t repeatCount = 1;
	if(variation == 4) {
		#ifdef DEBUG
		repeatCount = 100;
		#else 
		repeatCount = 10000;
		#endif
	}

	uint64_t factor = 1;
	uint64_t itemCount = 0;
	
	if (variation == 0) {
		itemCount = 1000;
	}
	else if(variation == 1) {
#ifdef DEBUG
		itemCount = 1000 * 100;
		factor = 1000 * 10;
#else
		itemCount = 1000 * 1000;
		factor = 1000 * 100;
#endif
		itemCount = itemCount / factor;
	}
	else if(variation == 2) {
		//itemCount = (1ll << 23);
		// Requires 64GB of memory to work, but after struggling at lot at 64GB, it will free ~20GB, then the remaining ~60GB when it finishes
		#ifdef DEBUG
		itemCount = (1ll << 18);
		#else
		itemCount = (1ll << 26);
		#endif
	}
	else if(variation == 3) {
		itemCount = (1ll << 14);
	} else if(variation == 4) {
		itemCount = (1ll << 10);
	} else {
		// Unhandled variation
		OnError(2);
	}

	TimeTrackerRoot rootTimer = { 0 };

	if (variation != 0 && variation != 1) {
		printf("insert + 2 gets + remove + dtor timing\n");
		Timing_StartRoot(&rootTimer);
	}


	for(uint64_t q = 0; q < repeatCount; q++) {

		tickGroupStart();
		for (uint64_t i = itemCount * threadIndex; i < itemCount * (threadIndex + 1); i++) {
			if (i == 209714) {
				//breakpoint();
			}
			tickStart();
			testAdd2(table, i, i, i);
			testAdd2(table, i, i, i);
			tickEnd();

			if (variation == 2) {
				if (i % (itemCount / 100) == 0) {
					printPercentTick("Add", table, (double)(i % itemCount + 1) / itemCount);
				}
			}

			if (variation == 0) {
				for (uint64_t j = 0; j <= i; j++) {
					uint64_t count = testGetCount2(table, j, j);
					AssertEqual(count, 2);
				}
			}
		}

		tickGroupStart();
		if (variation != 1) {
			for (uint64_t j = itemCount * threadIndex; j < itemCount * (threadIndex + 1); j++) {
				tickStart();
				/*
				{
					uint64_t count = testGetCount2(table, j + itemCount, j + itemCount);
					AssertEqual(count, 0);
				}
				*/
				{
					uint64_t count = testGetCount2(table, j, j);
					AssertEqual(count, 2);
				}
				tickEnd();

				if (variation == 2) {
					if (j % (itemCount / 100) == 0) {
						printPercentTick("Check", table, (double)(j % itemCount + 1) / itemCount);
					}
				}
			}
		}
		else {
			printf("get no matches (fast hashing):\n");

			uint64_t* hashes = new uint64_t[itemCount];
			for (uint64_t j = itemCount * threadIndex; j < itemCount * (threadIndex + 1); j++) {
				hashes[j] = getHash(j + itemCount, j + itemCount);
			}

			Timing_StartRoot(&rootTimer);

			

			for (uint64_t k = 0; k < factor; k++) {
				for (uint64_t j = 0; j < itemCount; j++) {
					typedef struct {
						Item item;
						uint64_t count;
					} Context;
					Context context = { 0 };
					context.item.a = j + itemCount;
					context.item.b = j + itemCount;

					uint64_t hash = hashes[j];
					// This call takes around 28 instructions, with the loop taking around 15 instructions
					int result = AtomicHashTable2_find(&table, hash, &context, [](void* contextAny, void* value) {
						Context* context = (Context*)contextAny;
						Item* item = (Item*)value;
						if (context->item.a == item->a && context->item.b == item->b) {
							context->count++;
						}
					});

					ErrorTop(result);

					AssertEqual(context.count, 0);
				}
			}
			Timing_EndRootPrint(&rootTimer, itemCount * factor);

			delete[] hashes;
			printf("get no matches:\n");
			Timing_StartRoot(&rootTimer);
			for (uint64_t k = 0; k < factor; k++) {
				for (uint64_t j = itemCount * threadIndex; j < itemCount * (threadIndex + 1); j++) {
					uint64_t count = testGetCount2(table, j + itemCount, j + itemCount);
					AssertEqual(count, 0);
				}
			}
			Timing_EndRootPrint(&rootTimer, itemCount * factor);

			printf("get all matches:\n");
			Timing_StartRoot(&rootTimer);
			for (uint64_t k = 0; k < factor; k++) {
				for (uint64_t j = itemCount * threadIndex; j < itemCount * (threadIndex + 1); j++) {
					uint64_t count = testGetCount2(table, j, j);
					AssertEqual(count, 2);
				}
			}
			Timing_EndRootPrint(&rootTimer, itemCount * factor);
		}


		tickGroupStart();
		for (uint64_t i = itemCount * threadIndex; i < itemCount * (threadIndex + 1); i++) {
			Item item = { 0 };
			item.a = i;
			item.b = i;
			item.c = i;

			tickStart();
			uint64_t removeCount = testRemove2(table, item);
			tickEnd();
			// It can be more than 2, due as we might have gotten it, decided to delete it, and then found it was moved, which will
			//	mean we will have to find it in the new allocation and check if we want to delete it again, resulting in many calls
			//	to the remove test function. We need at least 2 calls though, or else it clearly isn't deleting 2 entries.
			if (removeCount < 2) {
				OnError(3);
			}

			if (variation == 0) {
				for (uint64_t j = itemCount * threadIndex; j <= i; j++) {
					AssertEqual(testGetCount2(table, j, j), 0);
				}
				for (uint64_t j = i + 1; j < itemCount * (threadIndex + 1); j++) {
					AssertEqual(testGetCount2(table, j, j), 2);
				}
			}

			if (variation == 2) {
				if (i % (itemCount / 100) == 0) {
					printPercentTick("Remove", table, (double)(i % itemCount + 1) / itemCount);
				}
			}
		}
	}
	// Hmm... we should probably add "iterate all" function, because I am sure it would be useful to display
	//	the contents in a UI. Although... iterate all is problematic... Hmm... And maybe a clear all function?
	/*
	uint64_t allocSize = DebugAtomicHashTable2_allocationSize(&table);
	for(uint64_t i = 0; i < allocSize; i++) {
		AtomicHashTable2_remove(&table, i, nullptr, [](void* x, void* value){ x; value; return true; } );
	}
	*/
	if (variation != 0 && variation != 1) {
		Timing_EndRootPrint(&rootTimer, itemCount * factor * repeatCount);
	}
	
	printf("\t%f search pressure (1 means there were no hash collisions)\n", (double)table.searchLoops / (double)table.searchStarts);
}

void testSizingVar(int variation) {
	AtomicHashTable2 table = AtomicHashTableDefault(sizeof(Item), deleteItem);
	testSizingVarInner(table, variation, 0);
}

void testSizing() {
	testSizingVar(0);
	testSizingVar(1);
	testSizingVar(3);

	// Test to make sure we can scale large
	testSizingVar(2);
}

DWORD threadedSizing(TableMultiThreadsContext* context) {
	auto table = context->table;
	auto variation = context->variation;

	testSizingVarInner(*table, variation, context->threadIndex);

	return 0;
}

void* getValueHash(OutsideReference& ref) { return ((ref).valueForSet == 0 || (ref).isNull) ? 0 : (void*)(*(uint64_t*)((ref).pointerClipped + 32)); }
#define val(table, index) getValueHash(((AtomicHashTableBase*)(table.currentAllocation.pointerClipped + 32))->slots[index].value)

#include <vector>
void testHashChurn2VarInner(AtomicHashTable2& table, int variation, int threadIndex = 0) {

	uint64_t itemCount = 1000;
	uint64_t iterationCount = variation == 2 ? itemCount * 100 : itemCount * 10;


	int16_t* randomChoices = (int16_t*)malloc(iterationCount * sizeof(int16_t));
	int64_t* randomIndexes = (int64_t*)malloc(iterationCount * sizeof(int64_t));
	Item* items = (Item*)malloc(itemCount * sizeof(Item));


	if (variation == 0 || variation == 2) {
		randomBytes((unsigned char*)randomChoices, (int)(iterationCount * sizeof(int16_t)), 0x7cdd44b);
		randomBytes((unsigned char*)randomIndexes, (int)(iterationCount * sizeof(int64_t)), 0x7cdd44b);

		uint64_t offset = itemCount * threadIndex;
		for (uint64_t i = 0; i < itemCount; i++) {
			items[i].a = i + offset;
			items[i].b = i + offset;
			items[i].c = i + offset;
		}
	}
	else {
		randomBytesSecure((unsigned char*)randomChoices, (int)(iterationCount * sizeof(int16_t)));
		randomBytesSecure((unsigned char*)randomIndexes, (int)(iterationCount * sizeof(int64_t)));
		randomBytesSecure((unsigned char*)items, (int)(itemCount * sizeof(Item)));
	}


	


	std::vector<Item> itemsNotAdded(items, items + itemCount);
	std::vector<Item> itemsAdded;

	int16_t decision = 0;

	for(uint64_t i = 0; i < iterationCount; i++) {
		int64_t index = randomIndexes[i];
		if((decision >= 0 || itemsAdded.size() == 0) && itemsNotAdded.size() > 0) {
			index = index % itemsNotAdded.size();
			Item item = itemsNotAdded.at(index);
			uint64_t hash = getHash(item.a, item.b);
			//_CrtCheckMemory();
			testAdd2(table, item.a, item.b, item.c);
			//_CrtCheckMemory();
			uint64_t count = testGetCount2(table, item.a, item.b);
			if (count != 1) {
				uint64_t count2 = testGetCount2(table, item.a, item.b);
				breakpoint();
				if (count2 != 1) {
					breakpoint();
					AssertEqual(testGetCount2(table, item.a, item.b), 1);
				}
			}
			
			Item testGetItem = testGetSome2(table, item);
			if (variation == 2) {
				if (testGetItem.a != testGetItem.b || testGetItem.b != testGetItem.c) {
					// Corrupted item
					OnError(3);
				}
			}
			if (testGetItem.c != item.c) {
				Item testGetItem2 = testGetSome2(table, item);
				breakpoint();
				Item testGetItem3 = testGetSome2(table, item);
				AssertEqual(testGetItem.c, item.c);
			}
			//_CrtCheckMemory();
			itemsNotAdded.erase(itemsNotAdded.begin() + index);
			itemsAdded.push_back(item);
			//_CrtCheckMemory();
		} else {
			index = index % itemsAdded.size();
			Item item = itemsAdded.at(index);
			//_CrtCheckMemory();
			uint64_t count = testRemove2(table, item);
			//_CrtCheckMemory();

			// Count could be higher than 1, it just means we had to try a few times to remove it
			if (count < 1) {
				uint64_t count2 = testRemove2(table, item);
				AssertEqual(true, false);
			}

			//_CrtCheckMemory();
			itemsAdded.erase(itemsAdded.begin() + index);
			itemsNotAdded.push_back(item);
			//_CrtCheckMemory();
		}

		if(variation != 2)
		{
			for (uint64_t j = 0; j < itemsAdded.size(); j++) {
				Item item = itemsAdded.at(j);
				uint64_t hash = getHash(item.a, item.b);
				uint64_t count = testGetCount2(table, item.a, item.b);
				if (count != 1) {
					uint64_t count2 = testGetCount2(table, item.a, item.b);
					breakpoint();
					AssertEqual(count, 1);
				}
			}
			for (uint64_t j = 0; j < itemsNotAdded.size(); j++) {
				Item item = itemsNotAdded.at(j);
				uint64_t hash = getHash(item.a, item.b);
				uint64_t count = testGetCount2(table, item.a, item.b);
				if (count != 0) {
					uint64_t count2 = testGetCount2(table, item.a, item.b);
					breakpoint();
					AssertEqual(testGetCount2(table, item.a, item.b), 0);
				}
			}
		}
		decision += randomChoices[i] / 256;
		//_CrtCheckMemory();
	}

	for(uint64_t i = 0; i < itemsAdded.size(); i++) {
		Item item = itemsAdded.at(i);
		AssertEqual(testGetCount2(table, item.a, item.b), 1);
	}

	for(uint64_t i = 0; i < itemsAdded.size(); i++) {
		Item item = itemsAdded.at(i);
		uint64_t count = testRemove2(table, item);
		if (count < 1) {
			AssertEqual(true, false);
		}
	}

	
}

void testHashChurn2Var(int variation) {
	AtomicHashTable2 table = AtomicHashTableDefault(sizeof(Item), deleteItem);

	// Cause an allocation, to get the "empty, but has had data before" state (see the final zeroSnapshot check comment for why this matters)
	testAdd2(table, 1, 1, 1);
	Item item = { 0 };
	item.a = 1;
	item.b = 1;
	item.c = 1;
	testRemove2(table, item);


	testHashChurn2VarInner(table, variation);
}

DWORD threadedChurn(TableMultiThreadsContext* context) {
	auto table = context->table;
	auto variation = context->variation;

	testHashChurn2VarInner(*table, variation, context->threadIndex);

	return 0;
}

void testHashChurn2() {
	testHashChurn2Var(0);
	testHashChurn2Var(1);
	testHashChurn2Var(2);
}






void benchmarkCompareExchanges() {
	uint64_t count = 1000 * 1000 * 10;

	// TimeBlock(gets, uint64_t count = testGetCount(table, j + itemCount, j + itemCount);


	printf("64 bit:\n");
	{
		Timing_StartRoot(&rootTimer);
		volatile int64_t value = 0;
		for (uint64_t i = 0; i < count; i++) {
			InterlockedCompareExchange64(&value, value + 1, value);
		}
		Timing_EndRootPrint(&rootTimer, count);
	}

	printf("128 bit:\n");
	{
		Timing_StartRoot(&rootTimer);
		volatile __declspec(align(16)) LONG64 value[2] = { 0 };
		for (uint64_t i = 0; i < count; i++) {
			volatile __declspec(align(16)) LONG64 prevValue[2] = { value[0], value[1] };
			InterlockedCompareExchange128(value, value[1], value[0] + 1, (LONG64*)prevValue);
		}
		Timing_EndRootPrint(&rootTimer, count);
	}
}

/*
struct LinkedList {
	LinkedList* next;
};


#define BITS_IN_ADDRESS_SPACE 48

#pragma pack(push, 1)
struct PackedPointer {
	uint64_t pointerClipped: BITS_IN_ADDRESS_SPACE;
	uint64_t value : 64 - BITS_IN_ADDRESS_SPACE;
};
#pragma pack(pop)

#define RECOVER_POINTER(p) (((p) & (1ull << 47)) ? ((p) | 0xFFFF000000000000ull) : ((p) & 0x0000FFFFFFFFFFFFull))

struct LinkedList2 {
	PackedPointer next;
};

void benchmarkObfuscatedPointers() {
	uint64_t listSize = 1000;
	uint64_t iterationCount = 100000000ull / listSize;

	{
		LinkedList head = { 0 };
		{
			LinkedList* cur = &head;
			for (int i = 0; i < listSize - 1; i++) {
				auto p = new LinkedList();
				p->next = nullptr;
				cur->next = p;
				cur = p;
			}
		}

		printf("normal:\n");
		Timing_StartRoot(&rootTimer);
		for (int i = 0; i < iterationCount; i++) {
			LinkedList* cur = &head;
			int count = 0;
			while (cur) {
				count++;
				cur = cur->next;
			}
			if (count != listSize) {
				throw "wrong";
			}
		}
		Timing_EndRootPrint(&rootTimer, iterationCount * listSize);
	}


	{
		LinkedList head = { 0 };
		{
			LinkedList* cur = &head;
			for (int i = 0; i < listSize - 1; i++) {
				auto p = new LinkedList();
				p->next = nullptr;
				cur->next = (LinkedList*)((uint64_t)p & 0xFFFFFFFFFFFFull | (uint64_t)i << 48);
				cur = p;
			}
		}

		printf("funny pointers:\n");
		Timing_StartRoot(&rootTimer);
		for (int i = 0; i < iterationCount; i++) {
			LinkedList* cur = &head;
			int count = 0;
			while (cur) {
				count++;
				uint64_t p = (uint64_t)cur->next;
				LinkedList* next = (LinkedList*)((p & (1ull << 47)) ? (p | 0xFFFF000000000000ull) : (p & 0x0000FFFFFFFFFFFFull));
				cur = next;
			}
			if (count != listSize) {
				throw "wrong";
			}
		}
		Timing_EndRootPrint(&rootTimer, iterationCount * listSize);
	}

	{
		LinkedList2 head = { 0 };
		{
			LinkedList2* cur = &head;
			for (int i = 0; i < listSize - 1; i++) {
				auto p = new LinkedList2();
				p->next.pointerClipped = 0;
				cur->next.pointerClipped = (uint64_t)p;
				cur = p;
			}
		}

		printf("funny pointers 2:\n");
		Timing_StartRoot(&rootTimer);
		for (int i = 0; i < iterationCount; i++) {
			LinkedList2* cur = &head;
			int count = 0;
			while (cur) {
				count++;
				uint64_t p = cur->next.pointerClipped;
				LinkedList2* next = (LinkedList2*)RECOVER_POINTER(p);
				cur = next;
			}
			if (count != listSize) {
				throw "wrong";
			}
		}
		Timing_EndRootPrint(&rootTimer, iterationCount * listSize);
	}
}
*/

#include "RefCount.h"

// void Reference_Allocate(uint64_t size, OutsideReference* outRef, void** outPointer)

/*
void testTableMultiThreads(
	int count,
	int variationStart,
	int variationCount,
	DWORD(*runThread)(TableMultiThreadsContext*)
) {
*/

// 800kb... but it's just static memory, so that's nothing...
HANDLE threads[1000 * 100] = { 0 };
volatile LONG64 curThreadIndex = -1;

void SpawnThread(
	std::function<void()>* fnc
) {
	threads[InterlockedIncrement64(&curThreadIndex)] = CreateThread(
		0,
		0,
		// DWORD(*main)(void*)
		[](void* param) -> DWORD {
			auto fnc = (std::function<void()>*)param;
			(*fnc)();
			return 0;
		},
		fnc,
		0,
		0
	);
}
void WaitForAllThreads() {
	LONG64 threadIndex = 0;
	while (threadIndex <= curThreadIndex) {
		//printf("Waiting for thread %lld\n", threadIndex);
		DWORD error = WaitForSingleObject(threads[threadIndex], INFINITE);
		if (error) {
			printf("Wait error %d\n", GetLastError());
		}
		//printf("Finished thread %lld\n", threadIndex);
		threadIndex++;
	}
}


#pragma pack(push, 1)
typedef struct {
	union {
		uint64_t test;
		struct {
			int x;
			int y;
		};
	};
} Test;
#pragma pack(pop)

#include "MemPoolImpls.h"

void runRefCountTests() {	
	/*
	Test test = { 0 };



	uint64_t totalCount = 10000;
	uint64_t itemCount = 1;
	uint64_t iterationCount = totalCount / itemCount;
	uint64_t threadCount = 10;

	//todonext
	// Eh... I don't know. Something with iteration and lots of allocations, and removes values from slots.

	typedef struct {
		uint64_t value;
		int64_t thread;
	} Value;

	Timing_StartRoot(&rootTimer);

	OutsideReference* sharedValues = new OutsideReference[itemCount];
	memset(sharedValues, 0, itemCount * sizeof(OutsideReference));
	for (int t = 0; t < threadCount; t++) {
		SpawnThread(new std::function<void()>([=]() {
			OutsideReference* values = new OutsideReference[itemCount];
			memset(values, 0, itemCount * sizeof(OutsideReference));
			for (int i = 0; i < itemCount; i++) {
				Value* value;
				Reference_Allocate((MemPool*)&memPoolSystem, &values[i], (void**)&value, sizeof(Value), 0);
				value->value = 0;
				value->thread = t;
			}

			for (int k = 0; k < iterationCount; k++) {
				// Set all sharedvalues to our values
				for (int i = 0; i < itemCount; i++) {
					InsideReference* ourValue = Reference_Acquire(&values[i]);
					// ourValue has to exist, as nothing frees from the values array but us...

					InsideReference* value = Reference_Acquire(&sharedValues[i]);
					if (value) {
						Reference_DestroyOutside(&sharedValues[i], value);
						Reference_Release(&sharedValues[i], value);
					}
					
					Reference_SetOutside(&sharedValues[i], ourValue);
					Reference_Release(&values[i], ourValue);
				}

				// For all of our values which are still in the shared values, increment their value
				for (int i = 0; i < itemCount; i++) {
					InsideReference* value = Reference_Acquire(&sharedValues[i]);
					if (value) {
						Value* v = (Value*)Reference_GetValue(value);
						if (v->thread == t) {
							v->value++;
						}
						Reference_Release(&sharedValues[i], value);
					}
				}

				// Remove all of our values from the shared values
				for (int i = 0; i < itemCount; i++) {
					InsideReference* value = Reference_Acquire(&sharedValues[i]);
					if (value) {
						Value* v = (Value*)Reference_GetValue(value);
						if (v->thread == t) {
							Reference_DestroyOutside(&sharedValues[i], value);
						}
						Reference_Release(&sharedValues[i], value);
					}
				}
			}

			uint64_t totalCount = 0;
			for (int i = 0; i < itemCount; i++) {
				InsideReference* value = Reference_Acquire(&values[i]);
				Value* v = (Value*)Reference_GetValue(value);
				
				totalCount += v->value;

				Reference_DestroyOutside(&values[i], value);
				Reference_Release(&values[i], value);
			}

			printf("sum(value[%d])=%lld\n", t, totalCount);
		}));
	}


	WaitForAllThreads();

	Timing_EndRootPrint(&rootTimer, totalCount * threadCount);
	*/
}



void runAtomicHashTableTest() {
	//*
	// TODO: Add a debug wrapper for malloc and free, so we can track the number of allocations, and run tests to make sure we don't leak allocations.
	#ifdef DEBUG
	IsSingleThreadedTest = true;
	#endif

	testSizing();
	testHashChurn2();

	#ifdef DEBUG
	IsSingleThreadedTest = false;
	#endif
	//*/
	printf("ran single threaded tests\n");
	//testTableMultiThreads(4, 0, 1, threadedChurn);
	//testTableMultiThreads(4, 1, 1, threadedChurn);
	//testHashChurn2Var(2);
	//testSizingVar(4);

	testTableMultiThreads(2, 4, 1, threadedSizing);
	testTableMultiThreads(2, 2, 1, threadedChurn);
	testTableMultiThreads(16, 4, 1, threadedSizing);
	testTableMultiThreads(16, 2, 1, threadedChurn);

	//todonext
	// Oh, test with multiple threads... obviously...
}

int main() {
	try {
		runAtomicHashTableTest();
		//benchmarkCompareExchanges();
		//benchmarkObfuscatedPointers();
		//runRefCountTests();
	}
	catch (...) {
		printf("error main\n");
	}

	//runReadTest();

	return 0;
}