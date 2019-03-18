#include "simple_table.h"
#include "simple_table_requires.h"


bool InterlockedCompareExchange128Struct(
	void* structAddress,
	void* structOriginal,
	void* structNew
) {
	return InterlockedCompareExchange128(
		(LONG64*)structAddress,
		((LONG64*)structNew)[1],
		((LONG64*)structNew)[0],
		(LONG64*)structOriginal
	);
}

bool EqualsStruct128(
	void* structLhs,
	void* structRhs
) {
	// If the compare succeeds, it means lhs == rhs (and the set doesn't matter, as it only sets if they are equal,
	//	which then is a noop), and on success InterlockedCompareExchange128 returns 1, so this works.
	return InterlockedCompareExchange128(
		structLhs,
		structRhs,
		structLhs
	);
}


// A ref is
//	data (32 bits)
//	flags (some bits)
//	count (remaining bits)
// With count at the lowest bits (so increment works)
//	Also, this means if the count overflows, the whole system blue screens, so if that could happen,
//		we should probably add another check. But in my cases, that can't happen anyway...

#define REF uint64_t

// Outstanding calls
#define REF_COUNT(x) ((x) & ((1 << 26) - 1))
// Flags:
#define REF_FLAG_UNINITIALIZED (0 << 26)		// allowed: init
#define REF_FLAG_INITIALIZING (1 << 26)			// allowed: inited,dtor
#define REF_FLAG_INITIALIZED (2 << 26)			// allowed: add,remove,dtor
#define REF_FLAG_DESTRUCT_REQUEST (3 << 26)		// allowed: remove,dtor

// There is no start for being in the destruct state. We detect it via transitioning into DESTRUCT_REQUEST
//	with no refs, or via transitioning to no refs while in DESTRUCT_REQUEST. And as only one of these can
//	happen, and only once, until Ref_Destructed is called... so destruct is single threaded, and safe.

//		(whenever get isn't allowed, it just return nullptr. Others return non-zero values when not allowed).
//		(dtor when uninitialized fails is a noop, and dtor destructing is redundant)
//		(automatically enters destructing if any allocations fail)
#define REF_FLAGS(x) ((x) & (3 << 26))
#define REF_NON_FLAGS(x) ((x) & ~(3 << 26))

#define REF_DATA(x) ((x) >> 32)
#define REF_DATA_SET(x) ((x) << 32)


// Returns true if it can start initializing, and then call Ref_Inited
bool Ref_Init(uint64_t* ref) {
	return InterlockedCompareExchange(
		ref,
		REF_FLAG_INITIALIZING,
		REF_FLAG_UNINITIALIZED
	) == REF_FLAG_UNINITIALIZED;
}
// Should be called after Ref_Init returns true (and between the calls initialization of the resource should be done).
//	Returns true if the ref should be destructed, and Ref_Destructed should be called
//	Failures during init should result in freeing all previous allocated memory, and calling Ref_Destruct, then Ref_inited.
//		This will cause it to go back to uninitialized.
bool Ref_Inited(uint64_t* ref, uint32 data) {
	if (InterlockedCompareExchange(
		ref,
		REF_FLAG_INITIALIZED | REF_DATA_SET(data),
		REF_FLAG_INITIALIZING
	) != REF_FLAG_INITIALIZING) {
		// The only valid case is that destruct was called while initializing
		//	Otherwise, we are in a bad state, so should destruct anyway!
		*ref = REF_FLAG_DESTRUCT_REQUEST;
		return true;
	}
	return false;
}

// Returns false if it can't add a reference
bool Ref_Add(uint64_t* ref) {
	while (true) {
		uint64_t bits = *ref;
		if (REF_FLAGS(bits) != REF_FLAG_INITIALIZED
		) {
			return false;
		}
		if (InterlockedCompareExchange(ref, bits + 1, bits) == bits) {
			return true;
		}
	}
}

// Returns true if the reference should be destructed, and Ref_Destructed should be called.
bool Ref_Remove(uint64_t* ref) {
	while (true) {
		uint64_t bits = *ref;
		if (REF_FLAGS(bits) != REF_FLAG_INITIALIZED
			&& REF_FLAGS(bits) != REF_FLAG_DESTRUCT_REQUEST
		) {
			// Should be impossible...
			return false;
		}
		// Could be initialized, or destruct requested
		uint64_t newBits = bits - 1;
		if (InterlockedCompareExchange(ref, newBits, bits) != bits) continue;
		if (REF_COUNT(newBits) == 0 && REF_FLAGS(newBits) == REF_FLAG_DESTRUCT_REQUEST) {
			// We went from 1 to 0 refs, and destruct was requested (at some time). Therefore, we should destruct, as the ref
			//	could will never increment again (until we finish destruction), and so we have to destruct now.
			return true;
		}
		break;
	}
	return false;
}

// Marks the ref for deletion, and returns true if it should be destructed and Ref_Destructed be called.
bool Ref_Destruct(uint64_t* ref) {
	while (true) {
		uint64_t bits = *ref;
		if (REF_FLAGS(bits) != REF_FLAG_INITIALIZING
			&& REF_FLAGS(bits) != REF_FLAG_INITIALIZED
		) {
			return false;
		}
		uint64_t newBits = REF_NON_FLAGS(bits) | REF_FLAG_DESTRUCT_REQUEST;
		if (InterlockedCompareExchange(ref, newBits, bits) != bits) continue;

		if (REF_COUNT(newBits) == 0) {
			// The ref count can't increment now that we set DESTRUCT_REQUEST, so we have to destruct now,
			//	or else we will never destruct.
			return true;
		}

		break;
	}
	return false;
}

void Ref_Destructed(uint64_t* ref) {
	uint64_t bits = *ref;
	// Neither this if check nor the interlocked exchange should ever fail, if either does it means
	//	Ref_Destructed isn't being called after Ref_Remove/Ref_Destruct correctly.
	if (REF_COUNT(bits) == 0 && REF_FLAGS(bits) == REF_FLAG_DESTRUCTING) {
		// So... this could fail if we ever use forceful ref taking, which is possible during destruction
		//	and required to create a sorted list implementation (as this would require swaps,
		//	which forces us to reduce some locking requirements, as swaps interact with two refs at once).
		InterlockedCompareExchange(ref, REF_FLAG_UNINITIALIZED, bits);
	}
}



// Maximum (and minimum) bits for a single part of a transaction
#define TRANSACTION_ITEM_BITS 32

#define PENDING_TRANSACTION_LOG_MAX 1024
#define PENDING_TRANSACTION_LOG_INDEX_BITS 10

// Transaction guarantees
//	- Transactions will be applied only when the writes are based on a consistent and fresh state of the data,
//		and applyChange will be called for every part of the transaction, as many times as necessary until applyChange
//		returns successfully for every part of the transactions.
//	- Gets will only read consistent and fresh data states, and will try to apply writes before they run, and will rerun
//		if more writes have been created.
//		- The only drawback is that a write may be spurious applied multiple times, and so that will have to be prevented.
//		We provide an always increasing transaction id which facilitates ignoring spurious writes.

// NOTE: We could try to do this in 64 bits. It would require making newValue 8 bits, and then writing code to forcefully update
//	transaction ids in all user entries every once in a while, so we can tell the different between past entries, and future entries
//	(as if we wrap around they are indistinguishable, so the only way it to see what is more likely, and then update values enough
//		so the case of being the farther distance away becomes impossible).
//	- This could be done in read/write functions, which is actually safe because we can't wrap around without a lot of calls,
//		so if every call does some work to update transactionIds then we can provable keep them all fresh. It's a waste of
//		processing time though.
#pragma pack(push, 1)
typedef struct {
	uint64_t isEnd : 1;

	// 55 bits means at 2^30 per second, it would take 1 years for this to wrap around. That should be good enough...
	uint64_t transactionId : 55;

	todonext
	// So... dataIndex... we could use 6 bits of that to choose an allocation index in an allocation table,
	//	and then use use the remaining bits to choose an offset in that allocation.
	// The allocation index 0 is reserved to mean an offset into the base struct

	// The index in the data set this change should occur at.
	uint64_t dataIndex : 40;

	// The new value that will be set
	uint64_t newValue: 32;
} TransactionChange;
#pragma pack(pop)

#pragma pack(push, 1)
typedef struct {
	// Index of the head of the list.
	uint64_t startTransactionIndex : 32;
	uint64_t transactionCount : 32;

	// Means the transaction we are consuming we are also applying (otherwise we are discarding it).
	uint64_t valid : 1;
	// Means we are consuming a transaction, vs having just advanced start, and not having started consuming,
	//	as we don't know the transaction id of the next item, or if it is valid or not.
	uint64_t consuming : 1;
	// This is the id of the transaction we are consuming
	uint64_t transactionId : 62;
} TransactionQueueState;
#pragma pack(pop)


typedef __declspec(align(16)) struct {
	todonext
	// Initialize this to 0
	uint64_t nextTransactionId;

	TransactionQueueState state;
	TransactionChange pendingTransactions[PENDING_TRANSACTION_LOG_MAX];

	void* applyContext;
	// If it returns 1, it means it failed, but it is probably just a contention issue, and future changes should work
	// If it returns > 1, it means it failed, and all future changes will probably fail too
	// If change.transactionId is < any previous changes for change.dataIndex, the change should not be applied (and 1 should be returned).
	int(*applyChange)(void* applyContext, TransactionChange change);

} TransactionQueue;



todonext
// Oh yeah... we need to support this somehow... which may have to involve calling this in every retry loop...
//	Yeah, that's what we will need to do... call it at the start of all of our retry loops.
// private, only needs to be called internally, otherwise initialize with = {0}
void transactionQueue_ctor(TransactionQueue* this);

// Returns true on success
bool MutateTransactionState(
	TransactionQueueState* address,
	TransactionQueueState originalValue,
	TransactionQueueState newValue
) {
	return InterlockedCompareExchange128Struct(
		address,
		&originalValue,
		&newValue
	);
}
// Returns true on success
bool MutateChangeState(
	TransactionChange* address,
	TransactionChange originalValue,
	TransactionChange newValue
) {
	return InterlockedCompareExchange128Struct(
		address,
		&originalValue,
		&newValue
	);
}

// 0 on done
// 1 on retry
// > 1 on hard fail
int transactionQueue_applyTransactions(
	TransactionQueue* this,
	// The state that we use, which if we return 0 is the state we saw when we saw there was no more work left to do
	TransactionQueuedState* stateOut
) {
	TransactionQueuedState state = this->state;
	if (stateOut) {
		*stateOut = state;
	}

	// Quickly check if there is any work to do
	if (state.transactionCount == 0) {
		if (state.consuming) {
			TransactionQueuedState newState = state;
			newState.consuming = 0;
			MutateTransactionState(&this->state, state, newState);
		}
		// Empty, and we aren't consuming. So we are done.
		return 0;
	}
	
	if (!state.consuming) {

		uint64_t index = state.startTransactionIndex;
		uint64_t startTransactionId = this->pendingTransactions[index];

		bool isTransactionInvalid = false;

		uint64_t offset = 0;

		while (true) {
			uint64_t curIndex = (index + offset) % PENDING_TRANSACTION_LOG_MAX;
			TransactionChange change = this->pendingTransactions[curIndex];
			if (change.isEnd) {
				isTransactionInvalid = false;
				break;
			}
			// Hmm... this could compare across page boundaries, and as we aren't using a memory barrier, could
			//	this somehow fail to catch a change when there is one?
			if (change.transactionId != startTransactionId) {
				isTransactionInvalid = true;
				break;
			}
			offset++;
			if (offset == state.transactionCount) {
				// We have entries, but they are still being written, so we have nothing to do.
				return 0;
			}
		}

		// There is definitely an end (whether explicit, or because of a transactionId change), so we can enter consuming

		TransactionQueuedState newState = state;
		newState.consuming = 1;
		newState.valid = isTransactionInvalid ? 1 : 0;
		newState.transactionId = startTransactionId;
		MutateTransactionState(&this->state, state, newState);
		return 1;
	}


	// state.consuming is true, so we should consume something, or leave consuming state.


	TransactionChange change = this->pendingTransactions[state.startTransactionIndex];
	if (change.transactionId != state.transactionId) {
		// We are done consuming the previous transaction.
		TransactionQueuedState newState = state;
		newState.consuming = 0;
		// So... transitioning away from consuming on an index can only ever happen once. The transaction id
		//	should never appear again, and a transaction never uses two of the same index, so this is a unique transition.
		MutateTransitionState(&this->state, state, newState);
		// Now go back again and run the non consuming code (and on failure of the above set, retry again anyway).
		return 1;
	}

	int applyChangeResult = 0;
	if (!change.isEnd && state.valid) {
		applyChangeResult = this->applyChange(this->applyContext, change);
		// Even if applyChange fails, we still need to remove it from the queue!
		// This could happen if the applyChange applies the change, that process dies, and then another
		//	process calls this function and tries to apply the change again. In this case removing it from the queue
		//	is required. Also, restarting isn't necessary, but it doesn't hurt either, as it will just result in doing
		//	a few more checks to make sure we are in the right state, and then we will continue applying the changes.
	}

	// If we got here, it means the change succeeded. So try to remove it from the queue
	//	(if we fail to remove it from the queue someone else will apply it again, which is fine...
	//		and if someone else removes it, then our remove will fail, which is fine too).

	TransactionQueuedState newState = state;
	newState.startTransactionIndex = (newState.startTransactionIndex + 1) % PENDING_TRANSACTION_LOG_MAX;

	// So... we try to increase the position of the current transactions we are handling. As a transaction
	//	can't fill up the whole queue, and a transaction can only be added once, so this is a unique transition.
	MutateTransactionState(&this->state, state, newState);


	// If applyChange had a result, prefer to return that result
	if (applyChangeResult != 0) {
		return applyChangeResult;
	}

	// Whether we success or fail, we either made progress, or someone else made the progress we were trying to make.
	//	So run again, and make more progress.
	return 1;
}

int TransactionQueue_RunGetter(
	TransactionQueue* this,
	void* getterContext,
	// On non-zero returns, returns non-zero from our getter
	int(*getter)(void* getterContext)
) {
	do {
		int result = transactionQueue_applyTransactions(this, nullptr);

		if (result == 1) continue;
		if (result > 1) {
			return result;
		}
		// 0 means we checked, and there is no more transactions that can be applied.

		result = getter(getterContext);

		// If the getter failed, it likely means there was some contention during list resizing or something like that...
		//	so better go try to apply more transactions.
		if (result == 1) continue;

		if (result > 1) {
			return result;
		}

		// Okay, so... I believe we don't to store state before the getter and then check it again here to verify it hasn't changed.
		//	Because yes, our thread could block for an hour before we call getter, and then after we run it we can definitely
		//		see that something changed and that we should rerun our getter (and reapply transactions that are pending).
		//	HOWEVER, we could also block for an hour AFTER we check this state. And just as easily there could be all sorts of
		//		writes, and the caller might wonder "hey, I finished many writes and this getter just now returned and didn't
		//		say we have any of those writes", in which case our response is "well you started the get call before those
		//		writes even started, much less finished... clearly you have to call get after you call write to witness your changes".
	} while (false);

	return 0;
}



typedef struct {
	TransactionQueue* queue;
	uint64_t transactionId;
	TransactionQueuedState state;
	bool finishCalled;
} ApplyWriteState;
int TransactionQueue_applyWrite_insertWriteCallbackBase(ApplyWriteState* this, TransactionChange change, bool isEnd) {
	change.isEnd = isEnd ? 1 : 0;
	change.transactionId = this->transactionId;
	
	uint64_t insertIndex = (this->state.startTransactionIndex + this->state.transactionCount) % PENDING_TRANSACTION_LOG_MAX;
	TransactionChange pPrevChange = &this->queue->pendingTransactions[insertIndex];
	TransactionChange prevChange = *pPrevChange;
	if (!EqualsStruct128(this->state, this->queue->state)) {
		// Then something else was added/removed, so we need to retry
		return 1;
	}
	// Okay at this point... if any other transaction added after we checked state, then it will use a different transaction id.
	//	(We know no other transactions were added before we checked state, as that is what checking state does!)
	// And if it uses a different transaction id (than any used before), then it will not equal prevChange, so this will fail:
	if (!MutateChangeState(
		pPrevChange,
		prevChange,
		change
	)) {
		return 1;
	}

	// Now, try to confirm the change
	TransactionQueuedState newState = this->state;
	newState.transactionCount++;

	if (!MutateTransactionState(
		&this->queue->state,
		this->state,
		newState
	)) {
		// So... our previous write is fine. It won't be used, and then will be wiped out. And its transaction id won't collide
		//	with anything, because we are taking that transaction id to the grave with us.
		return 1;
	}
	
	this->state = newState;
	return 0;
}
int TransactionQueue_applyWrite_insertWriteCallback(ApplyWriteState* this, TransactionChange change) {
	return TransactionQueue_applyWrite_insertWriteCallbackBase(this, change, 0);
}

int TransactionQueue_applyWrite_finish(ApplyWriteState* this) {
	this->finishCalled = true;
	return TransactionQueue_applyWrite_insertWriteCallbackBase(this, {}, true);
}

todonext
// We need to add some handling for writes and gets fighting for contention so much they deadlock. This could easily happen with
//	2 cores and threads with the highest (kernel?) priority.
//	- Heck, even 2 writes could fight for contention.

int TransactionQueue_ApplyWrite(
	TransactionQueue* this,
	void* writeContext,
	int(*write)(
		void* writeContext,
		void* insertWriteContext,
		// If a non-zero returns, that result should be returned from the write function immediately
		int(*insertWrite)(
			void* insertWriteContext,
			// Only dataIndex and newValue are used in this.
			TransactionChange change
		),
		// If this is not called before write finishes, and write returns 0, we assume it is a hard failure
		// The result of this allows write to undo any changes if it isn't successfully applied
		int(*finish)(void* insertWriteContext)
	)
) {
	// If we ever retry in the middle of adding a transaction, that is fine. We will be adding new transaction parts when we loop
	//	around, which will implicitly cause the old transaction parts to be removed!
	do {
		TransactionQueuedState state;
		int result = transactionQueue_applyTransactions(this, &state);

		if (result == 1) continue;
		if (result > 1) {
			return result;
		}
		// 0 means we checked, and there is no more transactions that can be applied.

		// applyTransactions either says there is nothing queued to write, OR, every queued is still being written.
		// We assume anything that is still being written is from a hung thread, so we just insert anyway, implicitly
		//	causing those pending writes to be thrown out.

		ApplyWriteState writeState;
		writeState.queue = this;

		// Because we do this here, it guarantees the transaction ids of any valid transactions are always increasing.
		//	Any other transactions we are racing to add will only have one winner. Any transactions added will have gotten
		//	a nextTransactionId BEFORE we checked state (because it will be before they wrote to state, which has to be before
		//	us), and so BEFORE us, and so it will be below ours. So... this works!
		writeState.transactionId = InterlockedIncrement(&this->nextTransactionId);

		writeState.state = state;
		writeState.finishCalled = false;

		result = write(writeContext, &writeState, TransactionQueue_applyWrite_insertWriteCallbackBase, TransactionQueue_applyWrite_finish);
		if (result == 1) continue;
		if (result > 1) {
			return result;
		}

		if (!writeState.finishCalled) {
			return 2;
		}

		return 0;

	} while (false);

	return 0;
}


// Okay, allocations on top of 



todonext
// So... our actual data structure now has the transaction queue. This can guarantee transactions are applied non-interlaced,
//	when they have seen fresh data (and that gets will get non-interlaced data too) AND that they won't partially apply, and
//	writes and gets won't see partially applied data!
//	However, we still need to deal with a few things:
//	1) A change may be applied multiple times
//		- We fix this by storing transactionIds, and only applying a change if the transactionId increases the value
//	2) We may only queue a fixed number of changes at once
//		- This would break reallocation, but can fix this by slowly moving pieces on reallocation
//			- So... we can do it purely via communism. During reallocation every single write can be co-opted to move
//				two locations (at the end) to the new memory.
//	3) We can only write in increments of 32 bits...
//		- We will need to add macros to make it easier to deal with this

// We want a SmallPointerTable anyway, AND we want it to be allocating memory, that way it can guarantee it never double frees.

#pragma pack(push, 1)
typedef struct {
	void* pointer;
	uint64_t referenceCount;
} PointerReferenceStore;
#pragma pack(pop)

#pragma pack(push, 1)
typedef struct {

	uint64_t primaryAllocationIndex;
	uint64_t secondaryAllocationIndex;

	// Where sizes are 2^index
	//	Each pointer inside of here, is a PointerReferenceStore[] also... (or nullptr, and so not allocated.
	PointerReferenceStore allocationTable[64];

	// When this = the count of the primary allocation, we should make the secondary allocation the primary allocation,
	//	and wipe out the secondaryAllocation
	//	(this is only used to break up changes across many writes, to keep the number of pending transactions small)
	uint64_t nextPrimaryIndexToMove;

	// We check both the primary and secondary allocation, every time

	TransactionQueue transactions;

} SmallPointerTable;
#pragma pack(pop)


todonext
// Oh... so applyChange has to finish before moving to the next change OR until gets run. So... we should store
//	our memory with transactions somewhere, and then store it compactly (so we can look at it like a struct),
//	somewhere else. When we apply changes if the transaction value is >= we should copy to the compact memory.
//	- This also makes our reads REALLY nice, because it means reads can just read from the compact memory
todonext
//	- BUT WAIT A SECOND! Doesn't this then mean we don't even need to store the transaction id with the memory?
//		Couldn't we just set the transaction id, then try to set the other memory?
//		- Well... atomically set/increase the transaction id
//			- WAIT! Can't we use a universal transaction id...
todonext
// NO, that doesn't work... because we could hang after the transaction id


// And then we should provide macros/helper functions to write to the data, which can use struct offsets
//	to know where to write to.
// And also... we need to handle the dynamic allocations... in both cases... Maybe each dynamic allocation should
//	just have its own TransactionQueue? Hmm... nested TransactionQueues could get tricky though... I think there is
//	a problem with them I am not remembering right now...





// Insertions
//	- Find the end of the group, or if there is no end of the group, the end of the continous block
//		- If there is no end we have to lock all the group ends (of other groups) in the continous block
//		- Otherwise we just have to lock the end of our group
//	- Lock an empty entry after the end the group (of continous block)
//		- Also lock any entries that are also group ends for our group (so invalid)
//	- Wipe out any invalid group ends we encountered (of ours)
//	- Insert our value as a group end into that empty entry
//	- Make the original group end (or nothing if there wasn't one) no longer a group end

// Removal
//	- Lock everything starting from the first match, to the group end
//	- Start swapping our entries down, prioritizing entries farthest from us (but not group ends),
//		- Adding newly created spaces to the list of indexes to delete
//	- Keep doing this until we only have the group end left, then swap it down.
//	- We can do this in chunks, so we have a constant amount of indexes to keep track of, and after that we just stop
//		adding more, and instead loop again when we are done.

// Gets
//	- Iterate over the group, reset lock values, preventing past swaps into spaces we checked

// Resizing
//	- We can do this with conventional locks, so it can be safe
//	- We should start moving from the last values, so when a pure getter sees a redirect bit set, it
//		can switch iteration to the new allocation
//	- Our entries should be 64 bit, which will let us at atomically set the two entries in the target, putting
//		the source entry in the correct one of the two new entries
//	- Resizing down... is hard. It requires making sure every other entry is empty, so we can fit it into a smaller space.
//		- So... add a note about resizing down, but for now, we won't do it...
//	- So we lock both the target and original entries.

// Annoying states
//	- Duplicate group ends
//	- Values outside of 


// Basic operations
//	Lock retry block
//		- Sets/gets global mutate counter
//		- runs code
//		- At the end if the global mutate counter has changed, it reruns the code
//	Get
//		- Resets the data's mutate counter
//		- Importantly, doesn't restart the get if there was a mutate counter to wipe out, prioritizing gets over past
//			(likely dead interrupted) writes.
//		- May return:
//			- Redirect
//				- When we see this we start reading from the secondary allocation after this point
//			- Invalid
//				- We should get our primary allocation from the main one again, as it is no longer active
//			- Valid
//	Lock
//		- Sets the data's mutate counter
//		- All locks must occur before all writes
//	Write
//		- Must have the data's mutate counter set (via lock), and checks it against the current mutate counter
//		- If the current mutate is wrong, we can exit the current mutate attempt (or continue, but
//			none of writes will be applied, so there's no much of a point), restarting the lock retry loop



// Seems reasonable, supports up to 64GB of pointers (count, so really 512GB of pointers in data),
//	which is probably more than our algorithm can support anyway (without having every read and write contend
//	over data forever, never resolving).
#define BITS_TO_STORE_SMALL_POINTER_VALUE 36
// 2^b / interlocked exchange rate per second * 2^(b/2) = seconds until a collision
// 2^(b * 1.5) / (10^9) = seconds until a collision
//	Which means, if something is hitting the unique counter as fast as possible (causing it to loop around and essentially
//		become random) AND spawning threads, having them enter a mutation, then freezing the threads, and having them come back,
//		hoping they will see the same unique number and write, even though it was just luck it was the same number...
//		then it will take about 1.2 hours to collide. Because... it will take roughly 0.25s to loop the counter around,
//		and on average 2^14 (roughly 16 thousand) tries. But... eh... I want to support more bits in the table...
#define BITS_TO_STORE_UNIQUE_COUNTER 28




#pragma pack(push, 1)
typedef struct { // Must be <= 64 bits, as we need to be able to atomically set 2 of these at once
	uint64_t redirected : 1;
	uint64_t blockEnd : 1;
	uint64_t writeLock : BITS_TO_STORE_UNIQUE_COUNTER;
	uint64_t valueSmallPointer : BITS_TO_STORE_SMALL_POINTER_VALUE;
} DataEntry;
#pragma pack(pop)


#pragma pack(push, 1)
typedef struct { // Must be <= 128 bits
	uint64_t allocationSmallPointer : BITS_TO_STORE_SMALL_POINTER_VALUE;
	// 0 if we are not redirecting
	uint64_t secondaryAllocationSmallPointer : BITS_TO_STORE_SMALL_POINTER_VALUE;
	// Should be incremented BEFORE trying to use additional entries (but after locking them)
	//	And decremented AFTER we stop using entries. In this way, it will be conservative, so possibly higher than reality,
	//	but never lower (if it is lower, a add might enter, and infinitely loop because it can't find an entry).
	//	(and be lass than BITS_TO_STORE_SMALL_POINTER_VALUE, as not all of our small pointers are from values, but shouldn't be
	//		less than BITS_TO_STORE_SMALL_POINTER_VALUE).
	uint64_t claimedCount : BITS_TO_STORE_SMALL_POINTER_VALUE;
} AllocationEntry;
#pragma pack(pop)





#pragma pack(push, 1)
typedef struct { // Must be <= 128 bits
	uint64_t redirected : 1;
	uint64_t refCount: 63;
	uint64_t pointer;
} SmallPointerEntry;
#pragma pack(pop)


// TODO: Fix small pointer leaks
//	- So... these can leak. We should add a watchdog thread, and mutate counter, AND add more reference type mechanics,
//		maybe even two reference counts, so short duration references can be correctly identified as having leaked,
//		and be freed accordingly if nothing changes after a certain amount of time.

// So... the smaller pointer table, has to add its own allocations to its own table, so it can reference count them.
//	Of course it could just store reference counts of its own allocations independently, but... we already have a table for that...


// Small pointer table REF (128 bits, as we don't/can't move entries around)
//	(the 0 index can never be used, which we can implement by not using it, or subtracting 1 from input indicies)
//	- reference count (BITS_TO_COUNT_REFERENCES bits)
//	- original pointer (64 bits)
//	- pointer type (1 bit)
//		- either allocation, or value


typedef __declspec(align(16)) struct {
	// SmallPointerEntry[]
	AllocationEntry data;
} SmallPointerTable;

void SmallPointerTable_dtor(SmallPointerTable* this) {

}

// Only called internally, otherwise, initial SmallPointerTable like = {0}, or memset it to 0 before calling any functions.
// Returns false on allocation failures.
bool smallPointerTable_ctor(SmallPointerTable* this) {
	void* allocation = malloc(256);
	if (!allocation) {
		return false;
	}

	allocation

	return true;
}

// CREATING SMALL POINTER
//	- Automatically adds a reference count of 1
// Returns 0 on allocation failures.
uint64_t SmallPointerTable_createPointer(SmallPointerTable* this, void* pointer) {
	// We have to return a pointer using all the bits of BITS_TO_STORE_SMALL_POINTER_VALUE, as we may resize
	//	while they are using it, changing our actual index to use more bits (or all the bits).


}
// MAPPING SMALL POINTER
//	- Automatically adds a reference count of 1, requiring a dereference call after we are done
// Returns 0 if the pointer cannot be found.
void* SmallPointerTable_getPointer(SmallPointerTable* this, uint64_t smallPointer) {

}
// DEREFERENCING SMALL POINTER
void SmallPointerTable_dereference(SmallPointerTable* this, uint64_t smallPointer) {

}







typedef __declspec(align(8)) struct {
	volatile REF selfRef;

	uint64_t lockRetryCounter;

	AllocationEntry;



	// Total numbers of keys and values allocated.
	long count;

	bool(*compareLongKeys)(void* lhs, void* rhs);
	void(*destructValue)(void*);

	// We only allocate once, and we store that value here. All other allocations are a segment of this.
	void* allocation;

	// TODO: We need a watchdog mode, that checks for REFs that are destructing, and it sees a REF is in the destructing state
	//	for too long it can assume that the holder of the REF is dead, and forcefully destruct the REF. This may end up freeing
	//	memory another process was using, causing it to crash... but if it become unresponsive for a long period of time (seconds),
	//	crashing it isn't so bad.
	// This watchdog mode would need to be a check that is run in a mutual exclusive way on mutations (and the watchdog mode itself
	//	will have to be interuptable, as a process running the watchdog mode may also crash).
	// Also, this watchdog should look at allocations smaller than our current allocation, and free them if they don't die soon enough.


	// REF is enough, it gives 32 bits for the small key. This should be sufficient. If we eventually use a hashtable
	//	then we will take the low end bits, and so never even use the higher order bits in 64 bits. For our regular
	//	lists more bits may help prevent collisions, but... the difference should be neglible.
	REF* keys;
	void** values;
} ListFixedThreadSafe;



void ListFixedThreadSafe_dtorRefRequested(ListFixedThreadSafe* list) {
	void* allocation = list->allocation;
	// Might as call destructed before we free, to reuse the list as soon as possible? And in case free throws.
	Ref_Destructed(&list->selfRef);
	free(allocation);
}

// Returns non zero if it can't ctor it.
int ListFixedThreadSafe_ctor(
	ListFixedThreadSafe* list,
	long count,
	// Should compare longer keys stored inside of each value. Will only be called
	//	if the short key is equal, and is only called when both values are locked (so you don't
	//	need to check them for being freed).
	// Returns < 0 if lhs < rhs, > 0 if lhs > rhs and 0 if lhs == rhs
	int (*compareLongKeys)(void* lhs, void* rhs),
	// Is called when a value has been removed, or on all values when destruct is called.
	//	Calls to this may be delayed after the dtor of the list is called, to wait for outstanding calls to finish.
	void (*destructValue)(void*)
) {

	if (!Ref_Init(&list->selfRef)) {
		return 1;
	}

	list->usedCountMutateCountInterlocked = 0;
	list->count = count;
	list->compareLongKeys = compareLongKeys;
	list->destructValue = destructValue;

	long minSize = (sizeof(unsigned long) + sizeof(void*)) * count;
	long pages = minSize / PAGE_SIZE;
	if (minSize % PAGE_SIZE != 0) {
		pages++;
	}

	long size = pages * PAGE_SIZE;
	list->allocation = malloc(size);
	if (!list->allocation) {
		// Should always return true, but either way, we should bail.
		if (Ref_Destruct(&list->keys[index])) {
			// Because we only allocate once (and that failed) we don't need to do any cleanup.
			Ref_Destructed(&list->selfRef);
		}
		return 1;
	}
	memset(list->allocation, 0, size);

	list->keys = (unsigned long*)list->allocation;
	list->data = (unsigned char*)(list->keys + count);

	if (Ref_Inited(&list->selfRef)) {
		ListFixedThreadSafe_dtorRefRequested(list);
		return 1;
	}

	return 0;
}

void ListFixedThreadSafe_dtor(ListFixedThreadSafe* list) {
	if (Ref_Destruct(&list->selfRef)) {
		ListFixedThreadSafe_dtorRefRequested(list);
	}
}

// Returns -1 if it is not initialized
long ListFixedThreadSafe_getCurrentCount(ListFixedThreadSafe* list) {
	if (REF_FLAGS(list->selfRef) != REF_FLAG_INITIALIZED) {
		return -1;
	}
	return GetUsedCount(list->usedCountMutateCountInterlocked);
}



// Should be called to remove a ref from an item, as this will handle the cleanup, and list compression.
void listFixedThreadSafe_removeRef(ListFixedThreadSafe* list, int index) {
	void* value = list->values[index];

	// If it has more references, or just hasn't been requested to be destructed, then return
	if (!Ref_Remove(&list->keys[i])) {
		return;
	}
	list->destructValue(value);
	Ref_Destructed(&list->keys[i]);
	
	// TODO: Okay, the ideal system would work like this:
	//	- Every ref+pointer has a number specifying an allocation buffer and offset
	//	- The data should be treated like a tree
	//	- When mutating you should choose a node you want to replace, mark it, and make the new version of the tree
	//		under that node that you want, including old nodes if you want (and keeping track of all your allocations).
	//		- Then you try to atomically swap the node in. If you fail, free your new allocations, if you succeed, free your
	//			unused allocations.
	//	- When creating the new node you will have to set the redirect flags so that new gets write to the right place
	//	- Every redirected byte should increment a counter in the new node, which is then used to know when we can free it?
	//		- Eh... the redirected bytes may add some complications...
	//		- Wait... but...
	//		- We can chain further redirections (which works), but how do we ever collapse the direct chain, and copy the
	//			terminal data back to the base address, and make the base address stop redirecting?

	// Just a list
	//	- Just having a list, and then making the only operation be swaps of adjacent nodes (so making each node only 64 bits, which
	//		requires pointer remapping), works quite well for mutations.
	//		- Redirect bits will still be required for increasing allocation size, and different redirect bits on adjacent nodes
	//			prevents mutations, so redirect copying will still block.
	//		- Pointer remapping also requires a thread safe list, but this list can use much simpler refs, and never requires
	//			searching the list, and can't be compressed, so it isn't required to be compressed.
	//			- Oh... the pointer remap list can even grow specially, by having a "redirected count"
	//	- The only problem with this is... it makes deletions an O(N) operation
	//	- We can also make this handle full hashtables, which will actually speed up deletions (usually). They will need to use
	//		sentinel values to terminate collision lists, and maybe to start collision lists?

	// A tree
	//	- A tree is ideal, but it is hard to make and maintain a tree atomically. Sort of by nature a parent and its children
	//		will have to be stored in different memory locations...
	//	- A list, but with storage treating the list like a tree would work. Movement requires adding duplicates, but this should be fine...

	// Linked list
	//	- The fundamental problem with a linked list is that it is not connected enough. This means lookups won't be fast due to
	//		the structure, AND lookups can't just index directly into it like we can for a list. So it's just sort of slow...
	

	// Decrease used count while the last item is uninitialized (compress the list, while it is easy to compress).
	while (true) {
		uint64_t usedCountLocked = list->usedCountMutateCountInterlocked;
		int32_t usedCount = GetUsedCount(usedCountLocked);
		int32_t lastIndex = usedCount - 1;

		if (lastIndex < 0) {
			// No data to remove
			break;
		}

		if (list->keys[lastIndex] != REF_FLAG_UNINITIALIZED) {
			// It isn't uninitialized, so we can't free it
			break;
		}

		usedCount--;

		// If this fails because someone else is removing, we could break. Also if they add.
		//	However, they could also just be reusing an entry (and may or may not be reusing the last entry),
		//	so we should rerun the loop on success and failure of this, as eventually it will succeed (and
		//	every success changed usedCount), or one of the earlier checks will break out
		InterlockedCompareExchange(
			&list->usedCountMutateCountInterlocked,
			SetUsedCount(usedCountLocked, usedCount),
			usedCountLocked
		);
	}
}


// If the list is in a valid state, calls callback for the first match with key/exampleValue.
//	(also, as we don't allow duplicate to be added, this is the only match).
//	- The value passed to callback will be valid as long as callback runs, even if dtor is called
//		(we will delay dtor of at least that item until callback finishes).
//	- callback better not throw, or our list will leak memory, and possibly throw later on.
void ListFixedThreadSafe_get(
	ListFixedThreadSafe* list,
	uint32_t key,
	// Should have the full key filled out, so the compareLongKeys passed in the ctor can find a previously
	//	added value equal to exampleValue.
	void* exampleValue,
	void* callbackContext,
	void (*callback)(void* callbackContext, void* value)
) {
	int32_t usedCount = GetUsedCount(list->usedCountMutateCountInterlocked);

	if (usedCount == 0) {
		return;
	}
	// TODO: Initially use a default sized buffer that is statically allocated inside of our list for allocation,
	//	and with this initial buffer make it so keys and values always point to valid memory, even if dtor is called.
	// This would let us always iterate over keys to filter out keys that definitely don't match, without an interlocked and
	//	exchange on selfRef (until we think we might have a match), which could increase the speed of non matches on small
	//	lists by orders of magnitude (interlocked and exchange is fast, but just plain old value comparison is even faster).
	if (!Ref_Add(&list->selfRef)) {
		return;
	}

	// If usedCount increases, those will be adds that haven't completed (or started), and so we can ignore those.
	//	And if it decreases that is fine too, we can iterate over transitioning or uninitialized values fine
	//	(as long as usedCount is <= list->count, which it has to be?)
	for (int i = 0; i < usedCount; i++) {
		// We do this initial check without locking the ref to filter out fast cases
		//	(even with contention, if this is different, then we don't have to match the value,
		//	as either it IS different, or it is new enough we can ignore it).
		uint32_t listKeyUnsafe = REF_DATA(list->keys[i]);

		// If the small key is different, it is definitely different
		if (listKeyUnsafe != key) {
			continue;
		}

		// If we can't reference it, it isn't a good state (maybe not initialized, which means add hasn't finished),
		//	so we don't need to return it.
		if (!Ref_Add(&list->keys[i])) {
			continue;
		}

		void* value = list->values[i];
		// No need to check data from the ref again, as compareLongKeys will do that anyway.
		if (list->compareLongKeys(exampleValue, value) == 0) {
			callback(callbackContext, value);
			listFixedThreadSafe_removeRef(list, i);
			break;
		}

		listFixedThreadSafe_removeRef(list, i);
	}

	if (Ref_Remove(&list->selfRef)) {
		ListFixedThreadSafe_dtorRefRequested(list);
	}
}

// Returns:
//	0 if it could add it
//	1 if the list isn't initialized, or was destructing, etc
//	2 if the list is full
//	3 if the item is already in the list
// Returns false if it was not added successfully, which may be because we ran out of space,
//	the list was destructing, constructing, not constructed, or just because the element already existed.
int ListFixedThreadSafe_addInternal(ListFixedThreadSafe* list, uint32_t key, void* value) {
	if (key == 0) {
		key = 1;
	}

	// Used to prevent deadlocks, and in high contention situations just use more memory, or fail because we run out of memory.
	int indexOffset = 0;

	int addedIndex = -1;
	while (true) {
		uint64_t usedCountLocked;
		int32_t usedCount;
		// Atomically increment mutate, that way any outstanding calls will fail.
		{
			usedCountLocked = list->usedCountMutateCountInterlocked;
			usedCount = GetUsedCount(usedCountLocked);

			uint64_t newUsedCountLocked = usedCountLockedSetUsedCount(usedCountLocked, usedCount);	

			if (InterlockedCompareExchange(
				&list->usedCountMutateCountInterlocked,
				newUsedCountLocked,
				usedCountLocked
			) != usedCountLocked) {
				continue;
			}
			usedCountLocked = newUsedCountLocked;
		}

		int32_t index = usedCount + indexOffset;
		// Try different items, so we don't get stuck on an initializing item in a frozen thread
		//	(do this after here, so if we continue for any reason we try a different item).
		indexOffset++;

		if (index >= list->count) {
			return 2;
		}

		// Check for duplicates
		for (int i = 0; i < index; i++) {
			if (REF_DATA(list->keys[i]) != key) continue;
			if (!Ref_Add(&list->keys[i])) continue;
			if (REF_DATA(list->keys[i]) == key && list->compareLongKeys(list->values[i], value)) {
				// So... if it's a pending insert... 
				listFixedThreadSafe_removeRef(list, i);
				return 3;
			}
			listFixedThreadSafe_removeRef(list, i);
		}

		// Try to add it to the end of the list
		if (!Ref_Init(&list->keys[index])) continue;

		// After this point on any failures we need to make sure to destruct our item.
		list->values[index] = value;

		// We added something to the end of the list. Now let's see if we can add it atomically, as it might be the
		//	case that while we were adding it a duplicate of it was added!

		if (InterlockedCompareExchange(
			&list->usedCountMutateCountInterlocked,
			SetUsedCount(usedCountLocked, index + 1),
			usedCountLocked
		) != usedCountLocked) {
			// Destruct the item, to get it back into an uninitialized state.
			//	 May not return true if something was reading our value, but once it finishes reading, they will destruct the value for us.
			if (Ref_Destruct(&list->keys[index])) {
				Ref_Destructed(&list->keys[index]);
			}

			continue;
		}


		// Make the item available for use by finishing its init

		//	Failure isn't possible because calling remove while an add is pending means the remove
		//		on the add can be ignored, and in this case should be, as a remove shouldn't try to
		//		remove any refs it can't get a reference to!
		//	(and because it isn't possible, we don't actually destroy the value, as the remove caller couldn't
		//		have possibly correctly verified it was what they wanted to delete).
		if (Ref_Inited(&list->keys[index], key)) {
			Ref_Destructed(&list->keys[index]);
			continue;
		}

		return 0;
	}

	// Unreachable...
	return -1;
}
int ListFixedThreadSafe_add(ListFixedThreadSafe* list, uint32_t key, void* value) {
	if (!Ref_Add(&list->selfRef)) {
		return 1;
	}
	int returnCode = ListFixedThreadSafe_addInternal(list, key, value);
	if (Ref_Remove(&list->selfRef)) {
		ListFixedThreadSafe_dtorRefRequested(list);
		return 1;
	}
	return returnCode;
}


// No return. What would they want? To know if the item was removed? To synchronize an external list with this one?
//	That wouldn't work, and I can't think of any valid reasons for a return code.
void ListFixedThreadSafe_remove(ListFixedThreadSafe* list, unsigned long key, void* value) {
	// Because values can't move, we can just look for it and delete it. Add has to do extra work to prevent
	//	duplicates, but we just need to look through the list once and remove values. If anything is transitioning
	//	we can just pretend the transitioning is happening before/after us, so not our problem.

	if (!Ref_Add(&list->selfRef)) {
		return;
	}
	
	int32_t usedCount = GetUsedCount(list->usedCountMutateCountInterlocked);
	for (int i = 0; i < usedCount; i++) {
		if (REF_DATA(list->keys[i]) != key) continue;
		if (!Ref_Add(&list->keys[i])) continue;
		if (REF_DATA(list->keys[i]) == key && list->compareLongKeys(list->values[i], value)) {
			Ref_Destruct(&list->keys[i]);
		}
		listFixedThreadSafe_removeRef(list, i);
		// Keep iterating, there may be more than one item (even though that is really bad, as it means
		//	while iterating a previously unseen item will be available for a short time, which is flakey, but...
		//	it's probably better than not removing the item at all?)
	}

	if (Ref_Remove(&list->selfRef)) {
		ListFixedThreadSafe_dtorRefRequested(list);
	}
}




typedef struct {
	long count;
	unsigned long* keys;
	void** values;
} BaseValues;

typedef struct {
	//	0 uninitialized
	//	1 initializing
	//	2 initialized
	//	3 dead, because an allocate failed
	//	4 is destructing
	volatile long isInitialized;

	//	0 not resizing, use values
	//	1 resizing, use values
	//	2 resizing, and write to both values and resizingValues
	//	3 resizing, only use resizingValues

	// Okay, so a good pattern is to set a flag, everyone who reads the flags does both X+Y,
	//	then change that flag, so everyone is Y, and then when the last X+Y exits, do cleanup of X.

	// Hmm... how do we make even just "get" threadsafe even without resizing?
	//	I supposed... whenever we need to swap 2 items we can use 3 slots,
	//		making duplicates so a value always exists.
	//	And... we can queue work up, forcing mutating to be single threaded,
	//		while still allowing gets to be multi-threaded.

	// Hmm... so everyone writes to both values and resizingValues. But...
	//	how do they stop writing? I guess... we have to add handling so if you try
	//	to add an item that already exists (void* value is equal) then it has to noop,
	//	and similarily if we try to remove and item that doesn't, that is fine too.
	
	// Hmm... but what about the gap between checking the resizing flag and using
	//	values/resizingValues?

	

	volatile long isResizing;
	
	BaseValues values;
	// For when we are resizing, this is the new size, so if resizing, items have to be
	//	placed in here AND values
	BaseValues resizingValues;
} SimpleTable;

// Before expanding. Ex, 2 is 50%, 3 is 1/3 ~ 33%, etc.
long fillFactor = 3;


void SimpleTable_ctor(SimpleTable* table) {
	if (InterlockedCompareExchange(
		&table->isInitialized,
		1,
		0
	) != 0) {
		return;
	}
	
	table->isResizing = 0;
	table->values.count = 0;
	table->values.keys = nullptr;
	table->values.values = nullptr;
	table->resizingValues.count = 0;
	table->resizingValues.keys = nullptr;
	table->resizingValues.values = nullptr;

	resizeTable(table, PAGE_SIZE);

	table->isInitialized = 2;
}
// It is up to the caller of this to make sure all outstanding SimpleTable calls finish
//	BEFORE calling dtor.
void SimpleTable_dtor(SimpleTable* table) {

}

// Returns 0 if the add failed
long SimpleTable_add(unsigned long key, void* value);
void SimpleTable_remove(unsigned long key, void* value);
void* SimpleTable_get(unsigned long key, long(*equal)(void* lhs, void* rhs));


// Returns 0 if the resize failed
long resizeTable(SimpleTable* table, long targetCount) {
	if (InterlockedCompareExchange(
		&table->isResizing,
		1,
		0
	) != 0) {
		return 1;
	}


	//if(targetCount > )


	table->isResizing = 0;
} 