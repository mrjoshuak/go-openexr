//go:build amd64

#include "textflag.h"

// Constants for B44 sign-magnitude conversion
DATA b44_const_8000<>+0(SB)/8, $0x8000800080008000
DATA b44_const_8000<>+8(SB)/8, $0x8000800080008000
GLOBL b44_const_8000<>(SB), RODATA|NOPTR, $16

DATA b44_const_7c00<>+0(SB)/8, $0x7c007c007c007c00
DATA b44_const_7c00<>+8(SB)/8, $0x7c007c007c007c00
GLOBL b44_const_7c00<>(SB), RODATA|NOPTR, $16

DATA b44_const_ffff<>+0(SB)/8, $0xffffffffffffffff
DATA b44_const_ffff<>+8(SB)/8, $0xffffffffffffffff
GLOBL b44_const_ffff<>(SB), RODATA|NOPTR, $16

// func toOrderedSIMD(dst, src *[16]uint16)
// Converts 16 half-float values from sign-magnitude to ordered representation
// using SSE2 SIMD instructions.
//
// For each value:
// - NaN/Inf (exponent == 0x7c00): result = 0x8000
// - Negative (sign bit set): result = ^value
// - Positive: result = value | 0x8000
TEXT ·toOrderedSIMD(SB), NOSPLIT, $0-16
	MOVQ dst+0(FP), DI          // DI = dst pointer
	MOVQ src+8(FP), SI          // SI = src pointer

	// Load constants
	MOVOU b44_const_8000<>(SB), X1   // X1 = 0x8000 broadcast
	MOVOU b44_const_7c00<>(SB), X2   // X2 = 0x7c00 broadcast
	MOVOU b44_const_ffff<>(SB), X3   // X3 = 0xFFFF broadcast

	// Process first 8 values (bytes 0-15)
	MOVOU 0(SI), X0             // X0 = v[0:7]

	// Compute positive result: v | 0x8000
	MOVO X0, X4
	POR X1, X4                  // X4 = v | 0x8000

	// Compute negative result: ^v
	MOVO X3, X5
	PXOR X0, X5                 // X5 = ^v

	// Create negative mask: (v & 0x8000) == 0x8000
	MOVO X0, X6
	PAND X1, X6                 // X6 = v & 0x8000
	PCMPEQW X1, X6              // X6 = 0xFFFF if negative

	// Blend pos/neg: negative uses ^v, positive uses v|0x8000
	PAND X6, X5                 // X5 = ^v (only where negative)
	MOVO X6, X7
	PANDN X4, X7                // X7 = (v|0x8000) (only where positive)
	POR X7, X5                  // X5 = combined pos/neg result

	// Create NaN/Inf mask: (v & 0x7c00) == 0x7c00
	MOVO X0, X6
	PAND X2, X6                 // X6 = v & 0x7c00
	PCMPEQW X2, X6              // X6 = 0xFFFF if NaN/Inf

	// Blend with NaN/Inf result (0x8000)
	MOVO X1, X7
	PAND X6, X7                 // X7 = 0x8000 (only where NaN/Inf)
	PANDN X5, X6                // X6 = pos/neg result (where not NaN/Inf)
	POR X7, X6                  // X6 = final result for first 8 values

	MOVOU X6, 0(DI)             // Store first 8 results

	// Process second 8 values (bytes 16-31)
	MOVOU 16(SI), X0            // X0 = v[8:15]

	// Same sequence for second batch
	MOVO X0, X4
	POR X1, X4                  // X4 = v | 0x8000

	MOVO X3, X5
	PXOR X0, X5                 // X5 = ^v

	MOVO X0, X6
	PAND X1, X6
	PCMPEQW X1, X6              // X6 = neg mask

	PAND X6, X5
	MOVO X6, X7
	PANDN X4, X7
	POR X7, X5                  // X5 = pos/neg result

	MOVO X0, X6
	PAND X2, X6
	PCMPEQW X2, X6              // X6 = NaN/Inf mask

	MOVO X1, X7
	PAND X6, X7
	PANDN X5, X6
	POR X7, X6                  // X6 = final result

	MOVOU X6, 16(DI)            // Store second 8 results

	RET

// func findMaxSIMD(src *[16]uint16) uint16
// Finds the maximum value among 16 uint16 values.
// Uses SSE2-compatible unsigned max via XOR with 0x8000 trick.
TEXT ·findMaxSIMD(SB), NOSPLIT, $0-10
	MOVQ src+0(FP), SI          // SI = src pointer

	MOVOU 0(SI), X0             // X0 = values[0:7]
	MOVOU 16(SI), X1            // X1 = values[8:15]

	// For unsigned word max with SSE2: XOR with 0x8000, use signed max, XOR back
	// Load 0x8000 constant
	MOVOU b44_const_8000<>(SB), X4

	// Convert to signed range
	PXOR X4, X0
	PXOR X4, X1

	// Signed max of 8 words at a time
	// X0 = max(X0, X1) - now 8 values
	MOVO X0, X2
	PCMPGTW X1, X2              // X2 = X0 > X1 ? 0xFFFF : 0
	PAND X2, X0                 // Keep X0 where X0 > X1
	PANDN X1, X2                // Keep X1 where X1 >= X0
	POR X2, X0                  // X0 = max(X0, X1)

	// Reduce 8 -> 4: max of pairs
	MOVO X0, X1
	PSRLDQ $8, X1               // Shift right 8 bytes (4 words)
	MOVO X0, X2
	PCMPGTW X1, X2
	PAND X2, X0
	PANDN X1, X2
	POR X2, X0                  // X0[0:3] = max of all 8

	// Reduce 4 -> 2: max of pairs
	MOVO X0, X1
	PSRLDQ $4, X1               // Shift right 4 bytes (2 words)
	MOVO X0, X2
	PCMPGTW X1, X2
	PAND X2, X0
	PANDN X1, X2
	POR X2, X0                  // X0[0:1] = max of all 8

	// Reduce 2 -> 1: max of pair
	MOVO X0, X1
	PSRLDQ $2, X1               // Shift right 2 bytes (1 word)
	MOVO X0, X2
	PCMPGTW X1, X2
	PAND X2, X0
	PANDN X1, X2
	POR X2, X0                  // X0[0] = max of all 16

	// Convert back from signed
	PXOR X4, X0

	// Extract result
	MOVD X0, AX
	MOVW AX, ret+8(FP)
	RET

// func fromOrderedSIMD(dst, src *[16]uint16)
// Converts 16 values from ordered back to sign-magnitude representation.
// This is the inverse of toOrderedSIMD:
// - 0x8000 stays as 0x8000 (zero)
// - High bit set (ordered positive) -> clear high bit
// - High bit clear (ordered negative) -> NOT
TEXT ·fromOrderedSIMD(SB), NOSPLIT, $0-16
	MOVQ dst+0(FP), DI
	MOVQ src+8(FP), SI

	MOVOU b44_const_8000<>(SB), X1   // X1 = 0x8000 broadcast
	MOVOU b44_const_ffff<>(SB), X3   // X3 = 0xFFFF broadcast

	// Process first 8 values
	MOVOU 0(SI), X0             // X0 = ordered values

	// Compute positive result: v & ~0x8000 (clear high bit)
	MOVO X1, X4
	PXOR X3, X4                 // X4 = 0x7FFF
	MOVO X0, X5
	PAND X4, X5                 // X5 = v & 0x7FFF (positive result)

	// Compute negative result: ^v
	MOVO X3, X6
	PXOR X0, X6                 // X6 = ^v (negative result)

	// Create mask for high bit: (v & 0x8000) != 0
	MOVO X0, X7
	PAND X1, X7                 // X7 = v & 0x8000
	PCMPEQW X1, X7              // X7 = 0xFFFF if high bit set (positive in ordered)

	// Blend: use X5 where high bit set, X6 otherwise
	PAND X7, X5                 // X5 = positive result masked
	PANDN X6, X7                // X7 = negative result masked
	POR X5, X7                  // X7 = final result

	MOVOU X7, 0(DI)             // Store first 8 results

	// Process second 8 values
	MOVOU 16(SI), X0

	MOVO X1, X4
	PXOR X3, X4
	MOVO X0, X5
	PAND X4, X5

	MOVO X3, X6
	PXOR X0, X6

	MOVO X0, X7
	PAND X1, X7
	PCMPEQW X1, X7

	PAND X7, X5
	PANDN X6, X7
	POR X5, X7

	MOVOU X7, 16(DI)

	RET
