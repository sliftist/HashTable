#pragma once

#include "stdint.h"

#ifdef __cplusplus
extern "C" {
#endif



/*
* The Mersenne Twister pseudo-random number generator (PRNG)
*
* This is an implementation of fast PRNG called MT19937, meaning it has a
* period of 2^19937-1, which is a Mersenne prime.
*
* This PRNG is fast and suitable for non-cryptographic code.  For instance, it
* would be perfect for Monte Carlo simulations, etc.
*
* For all the details on this algorithm, see the original paper:
* http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/ARTICLES/mt.pdf
*
* Written by Christian Stigen Larsen
* Distributed under the modified BSD license.
* 2015-02-17, 2017-12-06
*/


/*
* We have an array of 624 32-bit values, and there are 31 unused bits, so we
* have a seed value of 624*32-31 = 19937 bits.
*/
#define MERSENNE_SIZE 624

// State for a singleton Mersenne Twister. If you want to make this into a
// class, these are what you need to isolate.
typedef struct {
	uint32_t MT[MERSENNE_SIZE];
	uint32_t MT_TEMPERED[MERSENNE_SIZE];
	size_t index;
} MTState;


/*
* Extract a pseudo-random unsigned 32-bit integer in the range 0 ... UINT32_MAX
*/
uint32_t mersenne_rand_u32(MTState*);

/*
* Initialize Mersenne Twister with given seed value.
*/
void mersenne_seed(MTState*, uint32_t seed_value);



#ifdef __cplusplus
}
#endif