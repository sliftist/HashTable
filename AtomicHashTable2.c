// So... transaction types

//todonext
// Go over the transactions, and what they need to store. I think we will have a few general purpose slots
//  for references, and then when we start the transaction get references to all those references,
//  so when we do our operations everything we swap will be non-recurring?
//  - But also... is a transaction queue even enough? We can use 128 bit swaps easily if we need it,
//      as our references are now 64 bit...

// So, transactions... they read atomically, and then can create a list of what they want to write
//  Spurious rights will always be possible though...

// How will inserts even work? The list head could easily go back to a previous state. Maybe... do we have to set
//  a flag in the entry once it has been inserted? But then... every time we move that is basically an insert...
//  unless we also set a flag for the move destination too (a different flag), and then reset that when
//  we finish moving, AND never reuse a memory block.
// Hmm... maybe inserts do need 128 bit compare and swap. Ugh... Didn't stroustrup doesn't solve this,
//  he just wipes out values, and then never reuses them, but we are extending, not just wiping out...

//todonext
// I think basically... we just want the transaction queue, but only on our list heads, etc. AND,
//  we can make our transactions larger, because the transactions themselves can be allocated
//  and memory tracked (with some reuse of them, for efficicency), which lets transactions
//  store a 64 destination, 64 bits to set, and 64 bits for the auto incrementing id.

// Maybe... when we delete the head we could just leave the entry there? Although even that doesn't work,
//  as entry reuse means a previously deleted may 

// Remove entry from linked list
//  