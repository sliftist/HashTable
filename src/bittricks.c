#include "bittricks.h"

// https://graphics.stanford.edu/~seander/bithacks.html
#define LT(n) n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n
static const char LogTable256[256] =
{
	-1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
	LT(4), LT(5), LT(5), LT(6), LT(6), LT(6), LT(6),
	LT(7), LT(7), LT(7), LT(7), LT(7), LT(7), LT(7), LT(7)
};
uint64_t log2_64(uint64_t v) {
	unsigned r;
	uint64_t t, tt, ttt;

	ttt = v >> 32;
	if (ttt) {
		tt = ttt >> 16;
		if (tt) {
			t = tt >> 8;
			if (t) {
				r = 56 + LogTable256[t];
			}
			else {
				r = 48 + LogTable256[tt];
			}
		}
		else {
			t = ttt >> 8;
			if (t) {
				r = 40 + LogTable256[t];
			}
			else {
				r = 32 + LogTable256[ttt];
			}
		}
	}
	else {
		tt = v >> 16;
		if (tt) {
			t = tt >> 8;
			if (t) {
				r = 24 + LogTable256[t];
			}
			else {
				r = 16 + LogTable256[tt];
			}
		}
		else {
			t = v >> 8;
			if (t) {
				r = 8 + LogTable256[t];
			}
			else {
				r = LogTable256[v];
			}
		}
	}
	return r;
}