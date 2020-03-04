#include "environment.h"

#include <Windows.h>

bool InterlockedCompareExchangeStruct128(
	void* structAddress,
	void* structOriginal,
	void* structNew
) {

	if ((LONG64)structAddress % 16 != 0) {
		OnError(4);
	}
	if ((LONG64)structOriginal % 16 != 0) {
		OnError(4);
	}
	if ((LONG64)structNew % 16 != 0) {
		OnError(4);
	}
	return cas(structAddress, structOriginal, structNew);
	/*
	LONG64 a = ((LONG64*)structAddress)[0];
	LONG64 b = ((LONG64*)structAddress)[1];
	LONG64 x = ((LONG64*)structOriginal)[0];
	LONG64 y = ((LONG64*)structOriginal)[1];
	if(a == x && b == y) {
		((LONG64*)structAddress)[0] = ((LONG64*)structNew)[0];
		((LONG64*)structAddress)[1] = ((LONG64*)structNew)[1];
		return true;
	}
	return false;
	*/
	//*
	return InterlockedCompareExchange128(
		(LONG64*)structAddress,
		((LONG64*)structNew)[1],
		((LONG64*)structNew)[0],
		(LONG64*)structOriginal
	);
	//*/
}
/*
00007FF634E65359  lock inc	qword ptr [r13+4C0h]  
00007FF634E65361  mov		 rax,qword ptr [rdi]  
00007FF634E65364  test		rax,rax  
00007FF634E65367  je		  testSizingVarInner+873h (07FF634E65403h)  
00007FF634E6536D  mov		 r9,0FFFFFFFFFFFFh  
00007FF634E65377  and		 rax,r9  
00007FF634E6537A  mov		 r8,rax  
00007FF634E6537D  or		  r8,r12  
00007FF634E65380  lock cmpxchg qword ptr [rdi],r8  
00007FF634E65385  jne		 testSizingVarInner+7FCh (07FF634E6538Ch)  
00007FF634E65387  and		 r8,r9  
00007FF634E6538A  jmp		 testSizingVarInner+811h (07FF634E653A1h)  
00007FF634E6538C  mov		 rcx,rdi  
00007FF634E6538F  call		Reference_Acquire (07FF634E6A290h)  
00007FF634E65394  mov		 r8,rax  
00007FF634E65397  mov		 r9,0FFFFFFFFFFFFh  
00007FF634E653A1  cmp		 qword ptr [r8+50h],0  
00007FF634E653A6  jne		 testSizingVarInner+855h (07FF634E653E5h)  
00007FF634E653A8  mov		 ecx,40h  
00007FF634E653AD  mov		 rdx,rbx  
00007FF634E653B0  sub		 cl,byte ptr [r8+28h]  
00007FF634E653B4  shr		 rdx,cl  
00007FF634E653B7  xor		 ecx,ecx  
00007FF634E653B9  lock inc	qword ptr [r13+4C8h]  
00007FF634E653C1  mov		 rax,qword ptr [r8+38h]  
00007FF634E653C5  mov		 rax,qword ptr [rax+rdx*8]  
00007FF634E653C9  test		rax,rax  
00007FF634E653CC  je		  testSizingVarInner+0FCBh (07FF634E65B5Bh)  
00007FF634E653D2  js		  testSizingVarInner+0F87h (07FF634E65B17h)  
00007FF634E653D8  and		 rax,r9  
00007FF634E653DB  cmp		 qword ptr [rax+18h],rbx  
00007FF634E653DF  jne		 testSizingVarInner+0F9Ah (07FF634E65B2Ah)  
00007FF634E653E5  mov		 rcx,r8  
00007FF634E653E8  and		 rcx,r9  
00007FF634E653EB  mov		 rax,rcx  
00007FF634E653EE  or		  rax,r12  
00007FF634E653F1  lock cmpxchg qword ptr [rdi],rcx  
00007FF634E653F6  je		  testSizingVarInner+873h (07FF634E65403h)  
00007FF634E653F8  mov		 rdx,r8  
00007FF634E653FB  mov		 rcx,rdi  
00007FF634E653FE  call		Reference_ReleaseX (07FF634E6A420h)  
00007FF634E65403  lea		 r9,[<lambda_c74c81948cff73800947299987d3b25e>::<lambda_invoker_cdecl> (07FF634E65C50h)]  
00007FF634E6540A  mov		 rdx,rbx  
00007FF634E6540D  lea		 r8,[rbp-60h]  
00007FF634E65411  mov		 rcx,r13  
00007FF634E65414  call		atomicHashTable2_findFull (07FF634E62A10h)  
00007FF634E65419  mov		 ebx,eax  
*/