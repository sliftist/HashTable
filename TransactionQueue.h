#pragma once

#include "environment.h"


#ifdef __cplusplus
extern "C" {
#endif

// TODO:
//	- Well... really the best way is probably to use mutual exclusive spinlocks.
//		The only time this fails is when a request comes in on a thread with a high IRQL. Because the request needs to be handled 'fast'.
//		But that is incorrect, as the high IRQL level means it requires more complicated (and slower) multithreading, and avoiding the
//		occasional context switch is not worth the tradeoff of ALWAYS having slower multithreading.
//		- However, we can resolve this issue. The spinlock can have a builtin failsafe where it unmaps the mapped memory the holder of the lock
//			is using to access it, therefore killing the holder of the lock, or at least preventing it from changing memory inside the lock.
//			- And at high IRQL levels we can automatically do this instead of waiting, or at least do it much sooner, assuming all other threads
//				may be paused while we are running.
//		- And... even though sometimes multiple cores can resolve the case of high irql locks spinlocking waiting for another thread,
//			this still consumes a core, and so if enough threads enter this state at once... every core will stay locked, so... high
//			irql is definitely bad.

// TODO: Generations and pointer swapping
//	(TranasctionQueue makes reads about 6 times slower than without TransactionQueue. And writes probably 100s of times slower.)
//	- Actually... the fastest way to do reads would be to use pointers, and then when we want to do writes we read
//		everything, mutate it, and swap it with the original pointer.
//		- This runs into problems though when we have a lot of data, making writes more expensive. You might think you could split
//			it up into a tree, BUT, then you run into the problem with one read accessing multiple tree nodes,
//			which lets to read part of a transaction (that writes across multiple tree nodes).
//		- So... probably the best solution involves our current solution, but with the data being pointers to larger chunks
//			of data...

// TODO: Dynamic allocation variant
//	- Right now we can't dynamically allocate from a static pool. Doing such would mean
//		Threads could claim memory, and then die before they connect their static allocations to the main queue.
//		(which would could result in total deadlock, if we run out of memory)
//	- However, with dynamic allocation we can just slowly use up system memory, of which GB should exist (instead of KB).
//	- With dynamic allocation we can atomically add a pointer to the queue, letting us handle interlaced operations
//		with greater ease.
//	- With this ability we could then support value comparisons when applying a transaction, which combined with
//		recording of the reads of a transaction would allow us to support specific transaction application, which only
//		rejects a transaction if data is specifically uses has changed.
//		- This in turn makes data contention far less of an issue, essentially removing "table lock", and using
//			cell locks instead. Which becomes best case scenario for most algorithms.
//	NOTE: This would also make transactions scale sublinearily, as right now each part of the transaction requires
//		multiple interlocked exchanges. If we could allocate memory we would only require an interlocked exchange
//		when for each write, and for adding/removing the transaction from the queue. And as we do 3 interlocked exchanges
//		per part we add to a transaction (and only 1 per write), this would make our code significantly faster...

// TODO: Page table manipulation
//	- There has to be some way to update the page table in order to prevent suspended/non-responsive processes
//		from writing to memory, by ripping the memory out from under them.
//	- Perhaps if each thread has it's own memory mapped file (would that give each thread different virtual addresses?)
//		that way we could just unmap the memory mapped file if a client isn't working fast enough.
//	- The benefit from this would be enormous, as we could then get rid of the fundamental intersperses interlocked compare exchanges,
//		which in itself would be fast, but by also using dynamic allocation could make transactions take a constant number
//		of interlocked compare exchanges, which will be lightning fast (and also not interspersing memory will be faster, and
//		use less memory).
//	- NOTE: I am not sure if virtual memory lookup results could be done, then context switched, and then after the write could still
//		happen even though the original virtual memory location is not in the page table (or in the cache). If so we could still
//		work around this by looking at what instruction the thread is on (and also somehow hooking into the thread starting again,
//		so we could know if it has moved), and then possibly switching back to the thread if it is about to write? Or something?
//		- Or we could always just kill the thread, or just set the irql high enough?

// TODO: Time based cancellation as a first class citizen
//	- So... if we have a pseudo-mutual exclusive lock, where threads have a stack allocated lock construct they set to a shared
//		pointer to claim the lock, and where they have to write the memory location they want to write to to that location
//		before starting to write. In this way other threads can go to that memory location and set the transaction id,
//		guaranteeing the write fails.
//		- Hmm... but is this any better than just having other threads enter? They probably won't run into contention? If threads
//			are optimistic they will probably work, and then threads won't require extra interlocked calls to update this
//			additional memory location... (and updating it will require interlocks, to deal with the time after we write,
//			when we need to see if we were cancelled before we try to record the next write time).
//			- Or... what if we check if we were cancelled, record where we want to write to, then check if we were cancelled again?
//				I think then... only the canceller needs to atomically set cancelled and get the write location?
//	- Yeah, this probably isn't too important, but it could make our cancellation a little more exact, which can make the cancellation
//		timing a bit better...

// TODO: Use the stack to record write history.
//	- This would make it possible to undo writes. We STILL need to intersperse transaction ids, BUT, because we can undo writes it means
//		we don't need a structure that is immune to being interrupted (as we will just rollback writes, so it will be like it never ran).
//		- This will greatly reduce our guarantees of progress... but using timing I believe we can prevent total stalemates, by essentially
//			just making it completely serial, except for time checks which catch cases when we were completely blocked, and then undo...


// Benchmarking
//	Eh... on my computer I found I get 57MB/s of bytes written without contention, and 13.5MB/s with maximized contention
//		(and I have 4 cores, so... this makes sense. Although it also means hyperthreading DOES NOT help our algorithm,
//		even without contention, which is bad...)


#define BYTES_PER_TRANSACTION 4
#define BITS_PER_TRANSACTION 32

#define BITS_IN_DATA_INDEX 40

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
typedef __declspec(align(16)) struct {
	uint64_t isEnd : 1;

	// 55 bits means at 2^30 per second, it would take 1 years for this to wrap around. That should be good enough...
	uint64_t transactionId : 55;

	// The index in the data set this change should occur at.
	//	A good usecase of this is to use 6 bits to lookup an allocation in an primary allocation table,
	//	and then use the remaining 34 bits to look up the offset inside that allocation. This allows resizing,
	//	and up to 16GB count of indexes.
	//uint64_t dataIndex : 40;
	uint64_t dataIndexHigh : 8;
	uint64_t dataIndexLow : 32;

	// The new value that will be set
	uint64_t newValue : BITS_PER_TRANSACTION;
} TransactionChange;
#pragma pack(pop)

CASSERT(sizeof(TransactionChange) == 16);

#define TransactionChange_set_dataIndex(x, value) { \
	(x)->dataIndexHigh = value >> 24; \
	(x)->dataIndexLow = value; /* Eh... the bitfield should just cut off the top bits... right? */ \
}

#define TransactionChange_get_dataIndex(x) (((x)->dataIndexHigh << 24) | ((x)->dataIndexLow))

// Where the end transaction marks counts as 1, so only this - 1 writes are allowed.
#define MAX_TRANSACTION_SIZE 256
#define MAX_TRANSACTION_INDEX_BITS 32

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
	// Index of the head of the list.
	uint64_t startTransactionIndex : MAX_TRANSACTION_INDEX_BITS;
	uint64_t transactionCount : MAX_TRANSACTION_INDEX_BITS;

	// Means the transaction we are consuming we are also applying (otherwise we are discarding it).
	uint64_t valid : 1;
	// Means we are consuming a transaction, vs having just advanced start, and not having started consuming,
	//	as we don't know the transaction id of the next item, or if it is valid or not.
	uint64_t consuming : 1;
	// This is the id of the transaction we are consuming
	//	(always increases, except when we are destructed).
	uint64_t transactionId : 55;
} TransactionQueueState;
#pragma pack(pop)

CASSERT(sizeof(TransactionQueueState) == 16);

#pragma pack(push, 1)
typedef struct {
	volatile uint64_t timedCalls;
	volatile uint64_t time;
	volatile uint64_t untimedCalls;

	//todonext
	// Actually... record logarithmic histograms for these, then run a long running run on 1 thread, to make sure we can
	//	detect thread switches. If we don't get thread switches then run many versions of the program, which will force thread
	//	switches.
	// Then add a context struct that staryRetryLoop can use so it can detect the starts of loops,
	//	and then on every retry have it record whether or not it retried due to contention, or thread switching.
	//	(oh, and make startRetryLoop actually loop on applyTransactions).

	// Hmm... so... if we get interrupted and have to restart, the fraction of "progress" we made vs how long it takes
	//	when we suceed measures our inefficiency rating... or something
	//	- Although, if it takes ~10%-20% more time, that doesn't mean cancelling it was okay...
	// Anyway, we can figure out how efficient it was to cancel it somehow, and then record that, and then use
	//	that value to decide how long to busy wait before cancelling (and I guess we would also need to busy wait on
	//	some "in progress" value... which could probably just a boolean
	// Oh, and there is also the time between cancelling, and the time the thread notices it was cancelled at. If that
	//	gap is large we definitely know the thread was switched away from, and that gap of time would be wasted if we waited.
	// Oh, and we need a function after startRetryLoop called "doneRetryLoop" to record times...
} CallTimes;
#pragma pack(pop)


// Start
//	- Fresh
//	- Was cancelled
//		- Have time cancel was called
//		- Have current time
//		- Worker that cancelled us (requires atomically taking the mutate lock and stuff, so we might not always have the worker that cancelled us?)
//			- May be finished
//			- May be pending
//			- May be cancelled
//	- Was repeatedly cancelled
// End
//	- Fresh
//	- From Cancelled

// Also... our model breaks down if we try to cancel a thread, BUT, it wins the underlying contention race and triggers
//	the canceller to retry. In this case the attempted canceller will try to double claim the lock, and the real
//	underlying code that's running won't have the lock claimed at all.
// So... in claiming the lock we reduce contention, but only have maybe 50% chance of actually getting it...


// These don't need to be atomic, so they aren't, and there are race conditions which could impact them.
//	They should generally give the correct numbers, resulting in good recommendations about cancelling and waiting,
//	which should result in low contention in the underlying resource.


// Remember to initialize to 0
typedef struct {
	uint64_t firstWaitTicks;
	uint64_t startTicks;
	uint64_t cancelCount;
	uint64_t id;
} CancelInfo;


#define CANCEL_TIMES_BUFFER_SIZE 256
typedef struct {
	uint64_t id;
	uint64_t cancelledAtTicks;
} CancelTimeEntry;

typedef struct {
	uint64_t currentId;
	uint64_t currentCancelCount;
	uint64_t currentStartTime;

	uint64_t ticksToWait;

	uint64_t prevId;

	// If we use this up, we just wrap around.
	CancelTimeEntry cancelEntries[CANCEL_TIMES_BUFFER_SIZE];
	uint64_t nextCancelEntryIndex;

	// Tracking info
	uint64_t lockFailures;
	uint64_t unneededCancels;
	uint64_t waitPollCount;

	// TODO: Maybe record histograms of these numbers, if they seem higher than they should histograms
	//	will give us a better idea about what is going on.
	uint64_t workTime;
	uint64_t blockedTime;
	uint64_t workFinishedCount;

} CancellableLock;

// NOTE:
//	Technically speaking, we don't queue transactions (I don't think). Because we run transactions before
//		we start writing, and fail writing if any transactions were added while we were writing, if we
//		are going to apply our transactions we will only do so on an empty queue, so only 1 valid transactions
//		can be queued at any time.

// Initialize with = {0} (there is no need to call a ctor)
//	When you are done with it, call TransactionQueue_dtor, if you want to reuse it in an entirely different context
//	(which cause most pending calls to return with a hard fail, but if a call is so fresh it hasn't actually entered it
//	can still get started, so dtor is of marginal usefulness).
#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
	// Put our 128 bit atomic structures at the top, so they are always aligned to 128 bits (16 bytes),
	//	assuming our struct itself is set aligned to 128 bits.

	TransactionQueueState state;

	
	TransactionChange pendingTransactions[MAX_TRANSACTION_SIZE];

	CancellableLock writeLock;

	volatile uint64_t nextTransactionId;

	volatile uint64_t retryLoopCount;
	volatile uint64_t applyCallCount;
	volatile uint64_t dequeueCount;
	volatile uint64_t contentionCount;

	CallTimes writeRecordCalls;
	CallTimes writeApplyCalls;
	CallTimes readCalls;

	volatile uint64_t hardFails;

	// So... we could have a logarithmic histogram for writes/reads and use that to decide when to interrupt vs wait?
	//	Then... we could have a factor which changes how long we decide to wait (maybe it decides the fraction
	//	in the histogram we choose), and we could change this factor... 

} TransactionQueue;
#pragma pack(pop)



int TransactionQueue_RunGetter(
	TransactionQueue* self,
	void* getterContext,
	// On non-zero returns, returns non-zero from our getter
	int(*getter)(void* getterContext),

	// Either this should have a reference to it before this function is called, or it should be static memory
	//	where the first thing applyChange does is get a reference to it. We won't save this... but we could hang
	//	before calling it, and our dtor does NOT block until all pending calls finish (so applyChange may be called
	//	after dtor is called).
	void* applyContext,
	// If it returns 1, it means it failed, but it is probably just a contention issue, and future changes should work
	// If it returns > 1, it means it failed, and all future changes will probably fail too
	// If change.transactionId is < any previous changes for change.dataIndex, the change should not be applied (and 1 should be returned).
	int(*applyChange)(void* applyContext, TransactionChange change)
);


int TransactionQueue_ApplyWrite(
	TransactionQueue* self,
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
		),
	// Either this should have a reference to it before this function is called, or it should be static memory
	//	where the first thing applyChange does is get a reference to it. We won't save this... but we could hang
	//	before calling it, and our dtor does NOT block until all pending calls finish (so applyChange may be called
	//	after dtor is called).
	void* applyContext,
	// If it returns 1, it means it failed, but it is probably just a contention issue, and future changes should work
	// If it returns > 1, it means it failed, and all future changes will probably fail too
	// If change.transactionId is < any previous changes for change.dataIndex, the change should not be applied (and 1 should be returned).
	int(*applyChange)(void* applyContext, TransactionChange change)
);


#ifdef __cplusplus
}
#endif