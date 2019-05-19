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
#include "MemLog.h"
#include "BulkAlloc.h"


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

#include <Windows.h>

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





#include "AtomicHashTable2.h"
#include "BulkAlloc.h"


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


BulkAlloc itemAllocator = BulkAlloc_Default();

typedef struct {
	uint64_t a;
	uint64_t b;
	uint64_t c;
} ItemInner;

typedef struct {
	ItemInner* item;
	ItemInner itemInline;
} Item;

// TODO: Oh, pass Item** in, so we will actually be simulating dynamic allocation and memory management.
void deleteItem(void* itemVoid) {
	//todonext
	// This is getting called multiple times, but... I have no idea how the first call is happening? There isn't a freeing inside ref call at all...
	//	it just... gets called...? On move out?

	Item* item = (Item*)itemVoid;

	if(item->item->a != item->itemInline.a
	|| item->item->b != item->itemInline.b
	|| item->item->c != item->itemInline.c) {
		// Item is corrupted
		OnError(3);
	}
	item->item->a = 0;
	item->item->b = 1;
	item->item->c = 2;

	MemLog_Add(nullptr, (uint64_t)item->item, "free", getHash(item->item->a, item->item->b));
	BulkAlloc_free(&itemAllocator, item->item);
}


void testAdd2(AtomicHashTable2& table, uint64_t a, uint64_t b, uint64_t c) {
	ItemInner itemInner = { 0 };
	itemInner.a = a;
	itemInner.b = b;
	itemInner.c = c;

	Item item = { 0 };
	item.itemInline = itemInner;

	item.item = (ItemInner*)BulkAlloc_alloc(&itemAllocator);
	*item.item = itemInner;

	uint64_t hash = getHash(a, b);

	MemLog_Add((const char*)(void*)&table, (uint64_t)item.item, "alloc", hash);

	ErrorTop(AtomicHashTable2_insert(&table, hash, &item));
}
uint64_t testRemove2(AtomicHashTable2& table, ItemInner& item
#ifdef ATOMIC_PROOF
,AtomicProof* proof = nullptr
#endif
) {
	uint64_t hash = getHash(item.a, item.b);
	uint64_t count = 0;

	FunctionWrapper<Item*, int> removeCallback([&](Item* pOther) {
		ItemInner other = pOther->itemInline;
		int shouldRemove = (int)(item.a == other.a && item.b == other.b && item.c == other.c);
		if (shouldRemove) {
			count++;
		}
		return shouldRemove;
	});
	
	int result = AtomicHashTable2_remove(&table, hash, removeCallback.getContext(), removeCallback.callbackFnc
		#ifdef ATOMIC_PROOF
		,proof
		#endif
	);
	ErrorTop(result);
	return count;
}
ItemInner testGetSome2(AtomicHashTable2& table, ItemInner& item) {
	uint64_t hash = getHash(item.a, item.b);
	ItemInner someItem = { 0 };

	FunctionWrapper<Item*, void> findCallback([&](Item* pOther) {
		if(pOther->item->a != pOther->itemInline.a
		|| pOther->item->b != pOther->itemInline.b
		|| pOther->item->c != pOther->itemInline.c) {
			// Item is corrupted
			OnError(3);
		}

		ItemInner other = pOther->itemInline;
		if(item.a == other.a && item.b == other.b && item.c == other.c) {
			someItem = other;
		}
	});

	int result = AtomicHashTable2_find(&table, hash, findCallback.getContext(), findCallback.callbackFnc);
	ErrorTop(result);
	return someItem;
}
uint64_t testGetCount2(AtomicHashTable2& table, uint64_t a, uint64_t b) {
	typedef struct {
		ItemInner item;
		uint64_t count;
	} Context;
	Context context = { 0 };
	context.item.a = a;
	context.item.b = b;

	uint64_t hash = getHash(a, b);

	int result = AtomicHashTable2_find(&table, hash, &context, [](void* contextAny, void* valueVoid) {
		Context* context = (Context*)contextAny;
		Item* pItem = ((Item*)valueVoid);
		ItemInner item = pItem->itemInline;
		if (context->item.a == item.a && context->item.b == item.b) {
			context->count++;
		}
		if(!BulkAlloc_isAllocated(&itemAllocator, pItem->item)) {
			// deleteItem was called while we were running
			OnError(3);
		}
		if(pItem->item->a != pItem->itemInline.a
		|| pItem->item->b != pItem->itemInline.b
		|| pItem->item->c != pItem->itemInline.c) {
			// Item is corrupted
			OnError(3);
		}
	});
	ErrorTop(result);
	return context.count;
}



#include "Timing.h"
#include "TimingDebug.h"


void checkForLeaks(
	std::function<void(AtomicHashTable2&)> const& run
) {
	MemLog_Init();
	BulkAlloc_ctor(&itemAllocator, sizeof(Item));
	AtomicHashTable2 table = AtomicHashTableDefault(sizeof(Item), deleteItem);


	run(table);


	printf("Final allocation count %llu\n", SystemAllocationCount);
	AtomicHashTable2_dtor(&table);
	BulkAlloc_dtor(&itemAllocator);
	if (SystemAllocationCount > 0) {
		OnError(3);
	}
	printf("Allocation count after dtor %llu\n", SystemAllocationCount);
	MemLog_Reset();

	#ifndef ATOMIC_HASH_TABLE_DISABLE_HASH_INSTRUMENTING
		printf("\t%f search pressure (1 means there were no hash collisions)\n", (double)table.searchLoops / (double)table.searchStarts);
	#endif
}

typedef struct {
	AtomicHashTable2* table;
	int variation;
	int threadIndex;
	HANDLE thread;
	int threadCount;
} TableMultiThreadsContext;
// Return result is meaningless
int testTableMultiThreads(
	int count,
	int variationStart,
	DWORD(*runThread)(TableMultiThreadsContext*)
) {
	checkForLeaks([&](AtomicHashTable2& table) {
		TableMultiThreadsContext* threads = new TableMultiThreadsContext[count];

		printf("Starting %d threads, variation %d\n", count, variationStart);

		memset(threads, 0, sizeof(HANDLE) * count);
		for (int i = 0; i < count; i++) {
			TableMultiThreadsContext* context = &threads[i];
			context->threadCount = count;
			context->table = &table;
			context->variation = variationStart;
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
		delete[] threads;
	});
	return 0;
}

uint64_t groupSum;
uint64_t groupCount;
uint64_t groupMax;
uint64_t groupMax2;
uint64_t tickSum;
uint64_t tickCount;
uint64_t tickMax;
uint64_t tickMax2;
void tickGroupStart() {
	groupSum = 0;
	groupCount = 0;
	groupMax = 0;
	groupMax2 = 0;
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
	uint64_t prevGroupMax = groupMax;
	groupMax = max(groupMax, curTime);
	if(curTime != groupMax) {
		groupMax2 = max(groupMax2, curTime);
		groupMax2 = max(groupMax2, prevGroupMax);
	}

	if (curTime > 108915300) {
		//breakpoint();
	}

	tickSum += curTime;
	tickCount++;
	uint64_t prevTickMax = tickMax;
	tickMax = max(tickMax, curTime);
	if(curTime != tickMax) {
		tickMax2 = max(tickMax2, curTime);
		tickMax2 = max(tickMax2, prevTickMax);
	}
}
void printPercentTick(const char* tickName, AtomicHashTable2& table, double fraction) {
	uint64_t count = DebugAtomicHashTable2_reservedSize(&table);
	uint64_t maxCount = DebugAtomicHashTable2_allocationSize(&table);
	printf("%s at %f%% %llu/%llu, %llu time %f%% of all, %llu average time, %llu average time - max, %f%% average of all, max %fX average, %f%% tick max of worst 2nd max, %f%% tick 2nd max of worst 2nd max, worst %f%%\n",
		tickName, fraction * 100, count, maxCount,
		tickSum,
		(double)tickSum / groupSum * 100,
		tickSum / tickCount,
		(tickSum - tickMax) / max(1, tickCount - 1),
		(double)(tickSum / tickCount) / (groupSum / groupCount) * 100,
		(double)tickMax / (tickSum / tickCount),
		(double)tickMax / groupMax * 100,
		(double)tickMax2 / groupMax2 * 100,
		(double)tickMax / tickSum * 100
	);
	tickSum = 0;
	tickCount = 0;
	tickMax = 0;
	tickMax2 = 0;
}


void testSizingVarInner(AtomicHashTable2& table, int variation, int threadIndex) {
	
	//todonext
	// Do all single threaded tests, and then do multithreaded tests, in debug and release...

	uint64_t repeatCount = 1;
	if(variation == 4) {
		repeatCount = 100;
	} else if(variation == 2) {
		#ifdef DEBUG
		repeatCount = 1;
		#else 
		repeatCount = 1;
		#endif
	}
	else if(variation == 0) {
		repeatCount = 10;
	}

	uint64_t factor = 1;
	uint64_t itemCount = 0;
	
	if (variation == 0) {
		itemCount = 600;
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
		//	EDIT: Changed to use less memory? Because we allocate these put thread, which can be upto 16 threads...
		#ifdef DEBUG
		itemCount = (1ll << 18);
		#else
		itemCount = (1ll << 21);
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
				for (uint64_t j = itemCount * threadIndex; j < i; j++) {
					uint64_t count = testGetCount2(table, j, j);
					if (count != 2) {
						uint64_t count2 = testGetCount2(table, j, j);
						breakpoint();
						uint64_t count3 = testGetCount2(table, j, j);
						AssertEqual(count, 2);
					}
					
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
					if (count != 2) {
						uint64_t count2 = testGetCount2(table, j, j);
						breakpoint();
						testGetCount2(table, j, j);
						AssertEqual(count2, 2);
					}
					
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
						ItemInner item;
						uint64_t count;
					} Context;
					Context context = { 0 };
					context.item.a = j + itemCount;
					context.item.b = j + itemCount;

					uint64_t hash = hashes[j];
					// This call takes around 27 instructions, With setting up context, the asserts and for loop around this taking around 14 instructions
					int result = AtomicHashTable2_find(&table, hash, &context, [](void* contextAny, void* value) {
						Context* context = (Context*)contextAny;
						Item* pItem = (Item*)value;
						ItemInner item = pItem->itemInline;
						if (context->item.a == item.a && context->item.b == item.b) {
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
			ItemInner item = { 0 };
			item.a = i;
			item.b = i;
			item.c = i;

			#ifdef ATOMIC_PROOF
			AtomicProof proof = { 0 };
			#endif
			tickStart();
			uint64_t removeCount = testRemove2(table, item
				#ifdef ATOMIC_PROOF
				,&proof
				#endif
			);
			tickEnd();
			// It can be more than 2, due as we might have gotten it, decided to delete it, and then found it was moved, which will
			//	mean we will have to find it in the new allocation and check if we want to delete it again, resulting in many calls
			//	to the remove test function. We need at least 2 calls though, or else it clearly isn't deleting 2 entries.
			if (removeCount < 2) {
				uint64_t countLeft = testGetCount2(table, i, i);
				if (countLeft > 0) {

					//printf("found error\n");
					//todonext
					// Add an uninitialized id which we autoincrement on malloc, so we can track versions of a table an filter them out across mallocs.
					//	Then pass that id (for the current and new allocation) back as proof from testRemove2, and then only print info on those
					//	ids (and the one before and after, to catch some recurrent allocation bugs).
					//	Also, start logging every index we check, and the values we saw at the indexes, and during inserts also log the inside references
					//		we inserted (and saw?).
					// OH! And also, have a fixed size array to record indexes we wrote to, and then also log every change of those indexes in those tables...
					// Then... that should be sufficient to completely know the relevant state, which should make debugging it trivial...

					//todonext
					// Or... maybe... pass a debug param to testRemove2, saying the number of removes we expect, and keep track of the current removes,
					//	and if we finish the newTable check, and are less than that, AND find the new table doesn't have another table after that...
					//	then breakpoint()?

					//todonext
					// Get the new allocation uint64_t before we call testRemove, and/or after it? Because we just really need the pointer value
					//	to get logsTableNew to write correctly, so we can see why the search didn't find the values in the curTable, even though they
					//	weren't in the newTable yet...
					// Yeah, the problem is probably that we couldn't find the value in curTable. The values were being moved while we were moving,
					//	but... that should be fine...
					#ifdef ATOMIC_PROOF
				

					uint64_t countLeft2 = testGetCount2(table, i, i);
					printf("left=%d, remove%d\n", countLeft2, removeCount);

					MemLog_SaveValuesTable("./logsTable.txt", proof.curTable.table, proof.curTable.mallocId, getHash(i, i));
					if(proof.newTable.table) {
						MemLog_SaveValuesTable("./logsTableNew.txt", proof.newTable.table, proof.newTable.mallocId, getHash(i, i));
					}

					uint64_t countLeft3 = testGetCount2(table, i, i);
					printf("left2=%d, remove%d\n", countLeft3, removeCount);

					#endif

					OnError(3);
					
				}
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
	if (variation != 0 && variation != 1) {
		Timing_EndRootPrint(&rootTimer, itemCount * factor * repeatCount);
	}
}

void testSizingVar(int variation) {
	AtomicHashTable2 table = AtomicHashTableDefault(sizeof(Item), deleteItem);
	uint64_t x = 0;
	testSizingVarInner(table, variation, 0);
}

void testSizing() {
	checkForLeaks([](AtomicHashTable2& table){ testSizingVarInner(table, 0, 0); });
	
	// Get speed tests
	// find operations
	checkForLeaks([](AtomicHashTable2& table){ testSizingVarInner(table, 1, 0); });
	// many operations
	checkForLeaks([](AtomicHashTable2& table){ testSizingVarInner(table, 3, 0); });

	// Test to make sure we can scale large
	checkForLeaks([](AtomicHashTable2& table){ testSizingVarInner(table, 2, 0); });
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

	uint64_t itemCount = 100;
	uint64_t iterationCount = variation == 2 ? itemCount * 100 : itemCount * 10;
	#ifndef DEBUG
	iterationCount = iterationCount * 100;
	#endif 


	int16_t* randomChoices = (int16_t*)malloc(iterationCount * sizeof(int16_t));
	int64_t* randomIndexes = (int64_t*)malloc(iterationCount * sizeof(int64_t));
	ItemInner* items = (ItemInner*)malloc(itemCount * sizeof(ItemInner));


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
		randomBytesSecure((unsigned char*)items, (int)(itemCount * sizeof(ItemInner)));
	}


	std::vector<ItemInner> itemsNotAdded(items, items + itemCount);
	std::vector<ItemInner> itemsAdded;

	int16_t decision = 0;

	for(uint64_t i = 0; i < iterationCount; i++) {
		int64_t index = randomIndexes[i];
		if((decision >= 0 || itemsAdded.size() == 0) && itemsNotAdded.size() > 0) {
			index = index % itemsNotAdded.size();
			ItemInner item = itemsNotAdded.at(index);
			uint64_t hash = getHash(item.a, item.b);
			//_CrtCheckMemory();
			testAdd2(table, item.a, item.b, item.c);
			//_CrtCheckMemory();

			auto tableBefore = table.currentAllocation;
			
			uint64_t count = testGetCount2(table, item.a, item.b);
			if (count != 1) {
				uint64_t count2 = testGetCount2(table, item.a, item.b);
				breakpoint();
				if (count2 != 1) {
					breakpoint();
					AssertEqual(testGetCount2(table, item.a, item.b), 1);
				}
			}
			
			ItemInner testGetItem = testGetSome2(table, item);
			if (variation == 2) {
				if (testGetItem.a != testGetItem.b || testGetItem.b != testGetItem.c) {
					// Corrupted item
					OnError(3);
				}
			}
			if (testGetItem.c != item.c) {
				ItemInner testGetItem2 = testGetSome2(table, item);
				breakpoint();
				ItemInner testGetItem3 = testGetSome2(table, item);
				AssertEqual(testGetItem.c, item.c);
			}
			//_CrtCheckMemory();
			itemsNotAdded.erase(itemsNotAdded.begin() + index);
			itemsAdded.push_back(item);
			//_CrtCheckMemory();
		} else {
			index = index % itemsAdded.size();
			ItemInner item = itemsAdded.at(index);
			//_CrtCheckMemory();
			uint64_t count = testRemove2(table, item);
			//_CrtCheckMemory();

			// Count could be higher than 1, it just means we had to try a few times to remove it
			if (count < 1) {
				breakpoint();
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
				ItemInner item = itemsAdded.at(j);
				uint64_t hash = getHash(item.a, item.b);
				uint64_t count = testGetCount2(table, item.a, item.b);
				if (count != 1) {
					uint64_t count2 = testGetCount2(table, item.a, item.b);
					breakpoint();
					AssertEqual(count, 1);
				}
			}
			for (uint64_t j = 0; j < itemsNotAdded.size(); j++) {
				ItemInner item = itemsNotAdded.at(j);
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
		ItemInner item = itemsAdded.at(i);
		AssertEqual(testGetCount2(table, item.a, item.b), 1);
	}

	for(uint64_t i = 0; i < itemsAdded.size(); i++) {
		ItemInner item = itemsAdded.at(i);
		uint64_t count = testRemove2(table, item);
		if (count < 1) {
			AssertEqual(true, false);
		}
	}

	free(randomChoices);
	free(randomIndexes);
	free(items);
}

void testHashChurn2Var(int variation) {
	AtomicHashTable2 table = AtomicHashTableDefault(sizeof(Item), deleteItem);

	// Cause an allocation, to get the "empty, but has had data before" state (see the final zeroSnapshot check comment for why this matters)
	testAdd2(table, 1, 1, 1);
	ItemInner item = { 0 };
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
	checkForLeaks([](AtomicHashTable2& table){ testHashChurn2VarInner(table, 0); });
	checkForLeaks([](AtomicHashTable2& table){ testHashChurn2VarInner(table, 1); });
	checkForLeaks([](AtomicHashTable2& table){ testHashChurn2VarInner(table, 2); });
}


#include "RefCount.h"


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

DWORD threadedItemContention(TableMultiThreadsContext* context) {
	AtomicHashTable2& table = *context->table;
	uint64_t itemCount = 1024;
	uint64_t iterationCount = 100;

	void (*findVerifyFnc)(void* contextAny, void* valueVoid) = [](void* contextAny, void* valueVoid) {
		Item* pItem = ((Item*)valueVoid);
		if(!BulkAlloc_isAllocated(&itemAllocator, pItem->item)) {
			// deleteItem was called on an item that is in use
			OnError(3);
		}
		if(pItem->item->a != pItem->itemInline.a
		|| pItem->item->b != pItem->itemInline.b
		|| pItem->item->c != pItem->itemInline.c) {
			// Item is corrupted
			OnError(3);
		}
		if(!BulkAlloc_isAllocated(&itemAllocator, pItem->item)) {
			// deleteItem was called on an item that is in use
			OnError(3);
		}
	};

	for(uint64_t k = 0; k < iterationCount; k++) {
		for(uint64_t i = 0; i < itemCount; i++) {
			testAdd2(table, i, i, i);
			AtomicHashTable2_find(&table, getHash(i, i), nullptr, findVerifyFnc);
		}
		for(uint64_t i = 0; i < itemCount; i++) {
			AtomicHashTable2_find(&table, getHash(i, i), nullptr, findVerifyFnc);
		}
		for(uint64_t i = 0; i < itemCount; i++) {
			AtomicHashTable2_find(&table, getHash(i, i), nullptr, findVerifyFnc);
			ItemInner item = { 0 };
			item.a = i;
			item.b = i;
			item.c = i;
			testRemove2(table, item);
			AtomicHashTable2_find(&table, getHash(i, i), nullptr, findVerifyFnc);
		}
	}
	return 0;
}



void runAtomicHashTableTest(bool ecoFriendly) {
	for (int i = 0; i < 100; i++) {
		testTableMultiThreads(16, 0, threadedItemContention);
	}

	// Speed test
	//testTableMultiThreads(1, 1, threadedSizing);
	//testTableMultiThreads(2, 2, threadedChurn);

	for (int i = 0; i < 100; i++) {
		//testTableMultiThreads(16, 2, threadedChurn);
	}
	//checkForLeaks([](AtomicHashTable2& table) { testSizingVarInner(table, 0, 0); });
	//checkForLeaks([](AtomicHashTable2& table){ testSizingVarInner(table, 1, 0); });

	for (int i = 0; i < 100; i++) {
		//testTableMultiThreads(2, 4, threadedSizing);
		//testTableMultiThreads(16, 2, threadedChurn);
		//testTableMultiThreads(2, 4, threadedSizing);
		//testTableMultiThreads(2, 4, threadedSizing);
		//testTableMultiThreads(2, 4, threadedSizing);
	}

	//testHashChurn2Var(2);

	//*
	//testSizingVar(1);

	// TODO: Add a debug wrapper for malloc and free, so we can track the number of allocations, and run tests to make sure we don't leak allocations.
	
	//*/
	
	//testTableMultiThreads(2, 4, threadedSizing);
	//testTableMultiThreads(10, 3, threadedSizing);

	// TODO: We really need a test that ACTUALLY tests contention on items, as right now we only have table contention, not item contention...
		/*
	for(uint64_t i = 0; i < 100; i++) {
		printf("start test look %d\n", i);
		#ifdef DEBUG
		IsSingleThreadedTest = true;
		#endif

		testSizing();
		testHashChurn2();

		#ifdef DEBUG
		IsSingleThreadedTest = false;
		#endif

		printf("ran single threaded tests\n");

		printf("0 to N with a lot of verification, a few times\n");
		testTableMultiThreads(1, 0, threadedSizing);
		testTableMultiThreads(2, 0, threadedSizing);
		!ecoFriendly && testTableMultiThreads(16, 0, threadedSizing);

		printf("churn medium amount of random items, many times\n");
		testTableMultiThreads(1, 2, threadedChurn);
		testTableMultiThreads(2, 2, threadedChurn);
		!ecoFriendly && testTableMultiThreads(16, 2, threadedChurn);

		printf("churn medium amount of random items, more random, many times\n");
		testTableMultiThreads(1, 1, threadedChurn);
		testTableMultiThreads(2, 1, threadedChurn);
		!ecoFriendly && testTableMultiThreads(16, 1, threadedChurn);

		printf("0 to N, many times\n");
		testTableMultiThreads(1, 3, threadedSizing);
		testTableMultiThreads(2, 3, threadedSizing);
		!ecoFriendly && testTableMultiThreads(16, 3, threadedSizing);

		for(uint64_t i = 0; i < 10; i++) {
			printf("0 to large N, many times\n");
			testTableMultiThreads(1, 4, threadedSizing);
			testTableMultiThreads(2, 4, threadedSizing);
			!ecoFriendly && testTableMultiThreads(16, 4, threadedSizing);
		}
	}
		//*/


	// many operations speed
	//testSizingVar(2);
	// get speed
	//testSizingVar(1);

	//todonext
	// Oh, test with multiple threads... obviously...

	printf("Final allocation count %llu\n", SystemAllocationCount);
}

int main(int argc, char** argv) {
	bool ecoFriendly = argc >= 2 && argv[1][0] == 'l';
	if (ecoFriendly) {
		printf("eco friendly\n");
	}
	try {
		runAtomicHashTableTest(ecoFriendly);
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