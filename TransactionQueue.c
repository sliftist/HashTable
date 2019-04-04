
#include "TransactionQueue.h"
#include "AtomicHelpers.h"
#include <intrin.h>

#include "Timing.h"

// Returns true on success
bool MutateTransactionState(
	TransactionQueueState* address,
	TransactionQueueState originalValue,
	TransactionQueueState newValue
) {
	return InterlockedCompareExchangeStruct128(
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
	return InterlockedCompareExchangeStruct128(
		address,
		&originalValue,
		&newValue
	);
}

uint64_t time() {
	unsigned int cpuid;
	return __rdtscp(&cpuid);
}


bool IsTicksMuchGreater(uint64_t lhs, uint64_t rhs) {
	// Add a base amount, as 10 ticks isn't much greater than 1, they are both marginal.
	return lhs / 4 > (rhs + 50);
}

// Ironically, just never waiting is ALMOST as good as waiting. Because our interlocks are almost like waiting anyway...
//	However waiting does measurably improve performance, so... it works.
//#define NO_WAITING

// Pre-emptive to prevent bad contention cases
void SmallTickIncrease(uint64_t* ticks) {
#ifndef NO_WAITING
	uint64_t ticksValue = *ticks;
	if (ticksValue < 5) {
		*ticks++;
	}
	else {
		*ticks = ticksValue * 5 / 4;
	}
#endif
}
// When we are fairly certain we weren't given enough time to complete before being cancelled
void MediumTickIncrease(uint64_t* ticks) {
#ifndef NO_WAITING
	uint64_t ticksValue = *ticks;
	if (ticksValue < 3) {
		*ticks += 2;
	}
	else {
		*ticks = ticksValue * 3 / 2;
	}
#endif
}

// Edge wait time downwards, to save time. But not too quickly, because when it crosses the actual time it takes to
//	do work we will waste time during contentious situations.
void SmallTickDecrease(uint64_t* ticks) {
	uint64_t ticksValue = *ticks;
	if (ticksValue < 20) {
		*ticks--;
	}
	else {
		*ticks = ticksValue * 19 / 20;
	}
}


void OnCancel(
	CancellableLock* lock,
	uint64_t cancelCount,
	uint64_t cancelledStartTime,
	uint64_t currentTime
) {
	// If it the first time cancelling, this is fine...
	if (cancelCount <= 0) {
		return;
	}

	// Increase ticksToWait more the higher cancelCount is...
	//todonext

	// This might not really be work time, as the worker may have been context switched, so not really working...
	// BUT, as this is the second time we have cancelled it, it seems more likely it was actually working.
	uint64_t cancelledWorkTime = currentTime - cancelledStartTime;

	if (IsTicksMuchGreater(cancelledWorkTime, lock->ticksToWait)) {
		// If a lot more work was done than ticksToWait it either means ticksToWait is too low, or work wasn't getting done,
		//	but it wasn't cancelled earlier because no one was waiting. We can distinguish the two cases, and we have to change
		//	ticksToWait now or else we will contend too much while we wait for the other thread to awaken, so... increase tickToWait by a bit.
		SmallTickIncrease(&lock->ticksToWait);
	}
}
void OnAwakenAfterCancel(
	CancellableLock* lock,
	uint64_t timeBeforeCancelling,
	uint64_t timeBeforeNoticingCancel
) {
	// If timeBeforeNoticingCancel is very low, it means we were progressing (probably), so ticksToWait should be higher
	//	Especially if previousTimeProgress is also low.
	// But if previousTimeProgress is very high... well that's impossible, as we would have been cancelled already?

	//if (timeBeforeNoticingCancel < lock->ticksToWait) {
	// Basically... if we responded faster than the current time we had before cancelling, then we responded
	//	pretty faster (probably < ticksToWait).
	//	- Unless some of our waiting time was when there was no contention. But even then, if there was no contention
	//		ticksToWait is probably 0 now, so we'll probably have to increase it now anyway.
	if (timeBeforeNoticingCancel < timeBeforeCancelling) {
		// It means we are responsive enough that we should have been allowed to just work, so ticksToWait has to be increased.
		MediumTickIncrease(&lock->ticksToWait);
	}
}
void OnFinish(
	CancellableLock* lock,
	CancelInfo* info
) {
	uint64_t finishTime = time();

	uint64_t workTime = finishTime - info->startTicks;
	uint64_t blockedTime = info->startTicks - info->firstWaitTicks;

	if (IsTicksMuchGreater(lock->ticksToWait, workTime)) {
		// We are waiting too long
		SmallTickDecrease(&lock->ticksToWait);
	}

	lock->workTime += workTime;
	lock->blockedTime += blockedTime;
	lock->workFinishedCount++;
}


void ClaimLockx(CancellableLock* lock, CancelInfo* claimer) { }
void ClaimLock(CancellableLock* lock, CancelInfo* claimer) {
	if (!claimer->id) {
		claimer->id = InterlockedIncrement64(&lock->prevId);
		claimer->firstWaitTicks = time();
	}

	if (lock->currentId == claimer->id) {
		// We lost the contention race with the original lock holder, so just try again...
		//	(or the ClaimLock logic was called twice when we already had the lock...)
		InterlockedIncrement64(&lock->lockFailures);
		return;
	}

	if (claimer->startTicks) {
		uint64_t currentTicks = time();

		// If we have previous started, and are back, that means we must have been cancelled.
		claimer->cancelCount++;

		// Search for the cancellation entry (which we may not find, because it might have taken us SOO long to wake up,
		//	that it has already been reused by someone else).


		// Default to startTicks. If it took so long to cancel that our cancel entry has been used up, we can just assume it took a maximum
		//	amount of time to cancel (and also, that we spent zero time previously in the fnc, which means cancelling it was free).
		uint64_t timeWeWereCancelled = claimer->startTicks;

		uint64_t endCancelIndex = lock->nextCancelEntryIndex;
		for (int offset = 1; offset <= CANCEL_TIMES_BUFFER_SIZE; offset++) {
			uint64_t cancelIndex = (endCancelIndex - offset + CANCEL_TIMES_BUFFER_SIZE) % CANCEL_TIMES_BUFFER_SIZE;
			CancelTimeEntry entry = lock->cancelEntries[cancelIndex];
			if (entry.id == claimer->id) {
				timeWeWereCancelled = entry.cancelledAtTicks;
				break;
			}
		}


		uint64_t timeBeforeCancelling = timeWeWereCancelled - claimer->startTicks;
		uint64_t timeBeforeNoticingCancel = currentTicks - timeWeWereCancelled;

		OnAwakenAfterCancel(lock, timeBeforeCancelling, timeBeforeNoticingCancel);
	}

	uint64_t startWaitTicks = 0;
	uint64_t currentId = 0;
	uint64_t currentCancelCount = 0;
	uint64_t currentStartTime = 0;

	while (true) {
		if (InterlockedCompareExchange64(&lock->currentId, claimer->id, 0) == 0) {
			// Got the lock safely
			lock->currentCancelCount = claimer->cancelCount;
			lock->currentStartTime = claimer->startTicks = time();
			break;
		}

		// Consider taking the lock, if we waited long enough

		// Set our start wait time, and reset it when the lock changes
		if (currentId != lock->currentId) {
			// Not atomic, but it should be fine, no amount of errors can cause a deadlock here.
			currentId = lock->currentId;
			currentCancelCount = lock->currentCancelCount;
			currentStartTime = lock->currentStartTime;
			startWaitTicks = time();
		}

		// It was just freed, so try to get it again
		if (!currentId) continue;

		uint64_t currentWaitTicks = time();

		uint64_t waitingTime = currentWaitTicks - startWaitTicks;
		if (waitingTime > lock->ticksToWait) {
			// We have waited long enough, now we want to hold the lock

			if (InterlockedCompareExchange64(&lock->currentId, claimer->id, currentId) == currentId) {
				// We stole the lock.
				lock->currentCancelCount = claimer->cancelCount;
				lock->currentStartTime = claimer->startTicks = currentWaitTicks;

				// Try to communicate the time we cancelled with the thread we cancelled. This is pretty rough, and could easily fail...
				//	but that's fine. We could use more interlockeds here, but it wouldn't really matter, as even if we record the cancelllation
				//	atomically, there is no reason to believe the cancelled thread will wake up fast enough to read it, considering
				//	we use a constant amount of memory.
				uint64_t entryIndex = InterlockedIncrement64(&lock->nextCancelEntryIndex) % CANCEL_TIMES_BUFFER_SIZE;
				lock->cancelEntries[entryIndex].cancelledAtTicks = currentWaitTicks;
				lock->cancelEntries[entryIndex].id = currentId;

				OnCancel(lock, currentCancelCount + 1, currentStartTime, currentWaitTicks);

				break;
			}
		}
		else {
			InterlockedIncrement64(&lock->waitPollCount);
		}
	}

}

void ReleaseLock(CancellableLock* lock, CancelInfo* claimer) {
	if (InterlockedCompareExchange64(&lock->currentId, nullptr, claimer->id) != claimer->id) {
		// There was probably an attempt to cancel us, but we actually finished. This can happen if we win the contention race, or
		//	are already so close to finishing when they cancel us.
		InterlockedIncrement64(&lock->unneededCancels);
	}
	
	OnFinish(lock, claimer);
}





void startTime(unsigned int* cpuid, uint64_t* ticks) {
	*ticks = __rdtscp(cpuid);
}
void finishTime(CallTimes* times, unsigned int* pcpuid, uint64_t* pticks) {
	//*
	unsigned int cpuid = *pcpuid;
	uint64_t ticks = *pticks;

	if (!ticks) {
		int zero = 0;
		// finishTime called multiple times
		OnError(3);
	}

	unsigned int cpuid2;
	uint64_t ticks2 = __rdtscp(&cpuid2);
	
	*pcpuid = nullptr;
	*pticks = nullptr;


	if (cpuid != cpuid2) {
		times->untimedCalls++;
	}
	else {
		times->timedCalls++;
		times->time += ticks2 - ticks;
	}
	//*/
}


// Causes most pending operations to return a hard fail, and all currently pending operations to not be applied
//	at any time after dtor returns, or in the future.
void TransactionQueue_dtor(TransactionQueue* this) {
	TransactionQueueState emptyState = { 0 };
	// So... we need to atomically set the 128 bits, (or else we could set half, have something else set, and then
	//	have the other half set), so loop to make sure at once time, the state is truly empty.
	//	(we could probably just set transactionId, but... this is nicer, I think).
	while (!MutateTransactionState(&this->state, this->state, emptyState));

	// No need to wipe out transactions, because we don't reset nextTransactionId, so any new transactions won't overlap.
	//	Also any pending inserts, gets, etc, will see that state.transactionId went down, and know our state is being reset.
}

// -1 on more items
// 0 on done
// 1 on retry
// > 1 on hard fail
int transactionQueue_applyTransactions(
	TransactionQueue* this,
	// The state that we use, which if we return 0 is the state we saw when we saw there was no more work left to do
	TransactionQueueState* state,
	void* applyContext,
	int(*applyChange)(void* applyContext, TransactionChange change)
) {
	// Quickly check if there is any work to do
	if (state->transactionCount == 0) {
		// Empty, so nothing to do (no point in reset the consuming flag, it will/has to reset when new values
		//	are added that will have different transaction ids).
		return 0;
	}

	if (!state->consuming) {

		uint64_t index = state->startTransactionIndex;
		uint64_t startTransactionId = this->pendingTransactions[index].transactionId;

		bool isTransactionInvalid = false;

		uint64_t offset;
		for (offset = 0; offset < state->transactionCount; offset++) {
			uint64_t curIndex = (index + offset) % MAX_TRANSACTION_SIZE;
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
		}
		if (offset == state->transactionCount) {
			// We have entries, but they are still being written, so we have nothing to do.
			return 0;
		}

		// There is definitely an end (whether explicit, or because of a transactionId change), so we can enter consuming

		//TransactionQueueState copy = *state;
		//TransactionQueueState thisCopy = this->state;

		TransactionQueueState newState = *state;
		newState.consuming = 1;
		newState.valid = isTransactionInvalid ? 0 : 1;
		newState.transactionId = startTransactionId;


		//bool invalid = startTransactionId <= thisCopy.transactionId;

		if (MutateTransactionState(&this->state, *state, newState)) {
			*state = newState;
			return -1;
		}
		else {
			return 1;
		}
	}


	// state->consuming is true, so we should consume something, or leave consuming state.


	TransactionChange change = this->pendingTransactions[state->startTransactionIndex];
	if (change.transactionId != state->transactionId) {
		// We are done consuming the previous transaction.
		TransactionQueueState newState = *state;
		newState.consuming = 0;
		// So... transitioning away from consuming on an index can only ever happen once. The transaction id
		//	should never appear again, and a transaction never uses two of the same index, so this is a unique transition.
		MutateTransactionState(&this->state, *state, newState);
		// Now go back again and run the non consuming code (and on failure of the above set, retry again anyway).
		return 1;
	}

	int applyChangeResult = 0;
	if (!change.isEnd && state->valid) {
		applyChangeResult = applyChange(applyContext, change);
		// Even if applyChange fails, we still need to remove it from the queue!
		// This could happen if the applyChange applies the change, that process dies, and then another
		//	process calls this function and tries to apply the change again. In this case removing it from the queue
		//	is required. Also, restarting isn't necessary, but it doesn't hurt either, as it will just result in doing
		//	a few more checks to make sure we are in the right state, and then we will continue applying the changes.
		InterlockedIncrement64(&this->applyCallCount);
	}

	// If we got here, it means the change succeeded. So try to remove it from the queue
	//	(if we fail to remove it from the queue someone else will apply it again, which is fine...
	//		and if someone else removes it, then our remove will fail, which is fine too).

	TransactionQueueState newState = *state;
	newState.startTransactionIndex = (newState.startTransactionIndex + 1) % MAX_TRANSACTION_SIZE;
	newState.transactionCount--;

	if (newState.transactionCount == 0) {
		newState.consuming = 0;
	}

	// So... we try to increase the position of the current transactions we are handling. As a transaction
	//	can't fill up the whole queue, and a transaction can only be added once, so this is a unique transition.
	if (MutateTransactionState(&this->state, *state, newState)) {
		InterlockedIncrement64(&this->dequeueCount);
		*state = newState;
		if (applyChangeResult == 0) {
			applyChangeResult = -1;
		}
	}
	else {
		// Only overwrite non-failures, otherwise keep hard failures
		if (applyChangeResult == 0) {
			applyChangeResult = 1;
		}
	}

	return applyChangeResult;
}

int transactionQueue_startRetryLoop(
	TransactionQueue* this,
	TransactionQueueState* state,
	void* applyContext,
	int(*applyChange)(void* applyContext, TransactionChange change)
) {
	// We claim the lock in the outer loop, even though this makes the time we lock variable, which could hurt performance during contention.
	//	However, this improves performance when we don't have contention... which is really more important.
	CancelInfo info = { 0 };
	while (true) {
		*state = this->state;
		if (state->transactionCount == 0) {
			return 0;
		}


		int result = 0;

		// Claim it late, to make the transactionCount == 0 case fast.
		// Claim the lock in every loop, as the only way we can loop again is if there is contention, which
		//	means someone must be cancelled our lock.
		ClaimLock(&this->writeLock, &info);

		while (true) {
			InterlockedIncrement64(&this->retryLoopCount);
			result = transactionQueue_applyTransactions(this, state, applyContext, applyChange);
			if (result != -1) break;
		}
		if (result == 1) continue;

		// Only release the lock when there is no contention otherwise our lock must have been cancelled.
		ReleaseLock(&this->writeLock, &info);
		return result;
	}
}

int TransactionQueue_RunGetterInner(
	TransactionQueue* this,
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
) {
	unsigned int cpuid;
	uint64_t ticks;
	startTime(&cpuid, &ticks);

	do {
		TransactionQueueState state;
		int result = transactionQueue_startRetryLoop(this, &state, applyContext, applyChange);

		if (result == 1) continue;
		if (result > 1) {
			InterlockedIncrement64(&this->hardFails);
			return result;
		}
		// 0 means we checked, and there are no more transactions that can be applied.

		TimeBlock(transactionsGetCallback, {
			result = getter(getterContext);
		});

		// If the getter failed, it likely means there was some contention during list resizing or something like that...
		//	so better go try to apply more transactions.
		if (result == 1) continue;

		if (result > 1) {
			InterlockedIncrement64(&this->hardFails);
			return result;
		}

		if (!EqualsStruct128(&state, &this->state)) {
			// We may have added more writes during the getter, and partially applied them, so we have to retry
			continue;
		}

		break;
	} while (true);

	finishTime(&this->readCalls, &cpuid, &ticks);
	return 0;
}

int TransactionQueue_RunGetter(
	TransactionQueue* this,
	void* getterContext,
	int(*getter)(void* getterContext),
	void* applyContext,
	int(*applyChange)(void* applyContext, TransactionChange change)
) {
	TimeBlock(transactionsGets,
	int result = TransactionQueue_RunGetterInner(this, getterContext, getter, applyContext, applyChange);
	);

	return result;
}

#pragma pack(push, 1)
typedef __declspec(align(16)) struct {
	TransactionQueue* queue;
	uint64_t transactionId;
	TransactionQueueState state;
	bool finishCalled;
} ApplyWriteState;
#pragma pack(pop)


uint64_t AtomicChangeCount = 0;

int TransactionQueue_applyWrite_insertWriteCallbackBase(ApplyWriteState* this, TransactionChange change, bool isEnd) {
	InterlockedIncrement64(&AtomicChangeCount);

	change.isEnd = isEnd ? 1 : 0;
	change.transactionId = this->transactionId;

	// Read write location (done via reading state when the algorithm starts)
	// Read write value
	// Verify write location is free/declare intent to write
	// Write/verify write value
	// Verify claim to write/confirm write


	uint64_t insertIndex = (this->state.startTransactionIndex + this->state.transactionCount) % MAX_TRANSACTION_SIZE;
	TransactionChange* pPrevChange = &this->queue->pendingTransactions[insertIndex];
	TransactionChange prevChange = *pPrevChange;

	// Declare our intent to write (via changing the transaction state)
	{
		TransactionQueueState newState = this->state;
		newState.transactionId = this->transactionId;
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
	}


	if (!MutateChangeState(
		pPrevChange,
		prevChange,
		change
	)) {
		return 1;
	}

	// Now, try to confirm the change
	{
		TransactionQueueState newState = this->state;
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
	}

	return 0;
}
int TransactionQueue_applyWrite_insertWriteCallback(ApplyWriteState* this, TransactionChange change) {
	TimeBlock(transactionWriteInsertCallback,
	int result = TransactionQueue_applyWrite_insertWriteCallbackBase(this, change, 0);
	);

	return result;
}

int TransactionQueue_applyWrite_finish(ApplyWriteState* this) {
	this->finishCalled = true;
	TransactionChange change = { 0 };
	return TransactionQueue_applyWrite_insertWriteCallbackBase(this, change, true);
}

// TODO: Add checks for multiple writes to the same location within a transaction. If that happens
//	it is possible for earlier writes to be the final writes applied... so... that is bad.
//	(although, the only way I can think of checking will also make the creation of writes quadratic in speed, which is bad)
// TODO: Add a warning if a write tries to write more values than PENDING_TRANSACTION_MAX. Currently if it does,
//	we will basically just (silently) drop the first entries that were added, only applies last entries that fit.
int TransactionQueue_ApplyWriteInner(
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
) {
	unsigned int cpuid;
	uint64_t ticks;
	startTime(&cpuid, &ticks);

	bool firstChance = true;

	// If we ever retry in the middle of adding a transaction, that is fine. We will be adding new transaction parts when we loop
	//	around, which will implicitly cause the old transaction parts to be removed!
	do {
		if (!firstChance) {
			InterlockedIncrement64(&this->contentionCount);
		}
		firstChance = false;

		TransactionQueueState state;
		int result = transactionQueue_startRetryLoop(this, &state, applyContext, applyChange);

		if (result == 1) continue;
		if (result > 1) {
			InterlockedIncrement64(&this->hardFails);
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
		writeState.transactionId = InterlockedIncrement64(&this->nextTransactionId);

		writeState.state = state;
		writeState.finishCalled = false;

		TimeBlock(transactionWriteInnerFnc,
		result = write(writeContext, &writeState, TransactionQueue_applyWrite_insertWriteCallback, TransactionQueue_applyWrite_finish);
		);


		if (result == 1) continue;
		if (result > 1) {
			InterlockedIncrement64(&this->hardFails);
			return result;
		}

		if (!writeState.finishCalled) {
			InterlockedIncrement64(&this->hardFails);
			return 2;
		}
		break;
	} while (true);

	finishTime(&this->writeRecordCalls, &cpuid, &ticks);
	startTime(&cpuid, &ticks);

	//*
	// And then, apply the changes
	do {
		TransactionQueueState state;
		int result = transactionQueue_startRetryLoop(this, &state, applyContext, applyChange);

		if (result == 1) continue;
		if (result > 1) {
			InterlockedIncrement64(&this->hardFails);
			return result;
		}
		break;
	} while (true);
	//*/

	finishTime(&this->writeApplyCalls, &cpuid, &ticks);

	return 0;
}


TimeTracker transactionWrites = { 0 };
int TransactionQueue_ApplyWrite(
	TransactionQueue* this,
	void* writeContext,
	int(*write)(
		void* writeContext,
		void* insertWriteContext,
		int(*insertWrite)(
			void* insertWriteContext,
			TransactionChange change
		),
		int(*finish)(void* insertWriteContext)
	),
	void* applyContext,
	int(*applyChange)(void* applyContext, TransactionChange change)
) {
	TimeBlock(transactionWrites,
	int result = TransactionQueue_ApplyWriteInner(this, writeContext, write, applyContext, applyChange);
	);

	return result;
}