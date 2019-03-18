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

//#include "simple_table.h"


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
			if(data[i] == data[0]) {
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

int main() {
	runInterlockedTest();

	return 0;
}