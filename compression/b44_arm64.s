//go:build arm64

#include "textflag.h"

// func toOrderedSIMD(dst, src *[16]uint16)
// Converts 16 half-float values from sign-magnitude to ordered representation
// using ARM NEON SIMD instructions.
//
// For each value:
// - NaN/Inf (exponent == 0x7c00): result = 0x8000
// - Negative (sign bit set): result = ^value
// - Positive: result = value | 0x8000
TEXT ·toOrderedSIMD(SB), NOSPLIT, $0-16
    MOVD dst+0(FP), R0          // R0 = dst pointer
    MOVD src+8(FP), R1          // R1 = src pointer

    // Load all 16 values (32 bytes)
    VLD1 (R1), [V0.H8, V1.H8]   // V0 = src[0:7], V1 = src[8:15]

    // Create constants
    MOVD $0x8000800080008000, R2
    VDUP R2, V4.D2              // V4 = 0x8000 broadcast
    MOVD $0x7c007c007c007c00, R2
    VDUP R2, V5.D2              // V5 = 0x7c00 broadcast

    // V20 = all 1s (for NOT operation via XOR)
    VMOVI $0xFF, V20.B16

    // Process first 8 values (V0)
    // ==============================

    // Compute positive result: v | 0x8000
    VORR V4.B16, V0.B16, V6.B16     // V6 = v | 0x8000

    // Compute negative result: ^v (XOR with all 1s)
    VEOR V20.B16, V0.B16, V7.B16    // V7 = ^v

    // Create negative mask: (v & 0x8000) == 0x8000
    VAND V4.B16, V0.B16, V8.B16     // V8 = v & 0x8000
    VCMEQ V4.H8, V8.H8, V8.H8       // V8 = 0xFFFF if negative

    // Blend pos/neg: result = (mask & negResult) | (~mask & posResult)
    // ~mask via XOR with all 1s, then AND
    VAND V8.B16, V7.B16, V10.B16    // V10 = mask & negResult
    VEOR V20.B16, V8.B16, V11.B16   // V11 = ~mask
    VAND V11.B16, V6.B16, V11.B16   // V11 = posResult & ~mask
    VORR V10.B16, V11.B16, V6.B16   // V6 = blended result

    // Create NaN/Inf mask: (v & 0x7c00) == 0x7c00
    VAND V5.B16, V0.B16, V9.B16     // V9 = v & 0x7c00
    VCMEQ V5.H8, V9.H8, V9.H8       // V9 = 0xFFFF if NaN/Inf

    // Blend with NaN/Inf result (0x8000)
    // result = (mask & 0x8000) | (~mask & previous)
    VAND V9.B16, V4.B16, V10.B16    // V10 = mask & 0x8000
    VEOR V20.B16, V9.B16, V11.B16   // V11 = ~mask
    VAND V11.B16, V6.B16, V11.B16   // V11 = previous & ~mask
    VORR V10.B16, V11.B16, V6.B16   // V6 = final result for first 8

    // Process second 8 values (V1)
    // ==============================

    // Compute positive result: v | 0x8000
    VORR V4.B16, V1.B16, V12.B16    // V12 = v | 0x8000

    // Compute negative result: ^v
    VEOR V20.B16, V1.B16, V13.B16   // V13 = ^v

    // Create negative mask
    VAND V4.B16, V1.B16, V14.B16    // V14 = v & 0x8000
    VCMEQ V4.H8, V14.H8, V14.H8     // V14 = 0xFFFF if negative

    // Blend pos/neg
    VAND V14.B16, V13.B16, V10.B16  // V10 = mask & negResult
    VEOR V20.B16, V14.B16, V11.B16  // V11 = ~mask
    VAND V11.B16, V12.B16, V11.B16  // V11 = posResult & ~mask
    VORR V10.B16, V11.B16, V7.B16   // V7 = blended result

    // Create NaN/Inf mask
    VAND V5.B16, V1.B16, V15.B16    // V15 = v & 0x7c00
    VCMEQ V5.H8, V15.H8, V15.H8     // V15 = 0xFFFF if NaN/Inf

    // Blend with NaN/Inf result
    VAND V15.B16, V4.B16, V10.B16   // V10 = mask & 0x8000
    VEOR V20.B16, V15.B16, V11.B16  // V11 = ~mask
    VAND V11.B16, V7.B16, V11.B16   // V11 = previous & ~mask
    VORR V10.B16, V11.B16, V7.B16   // V7 = final result for second 8

    // Store results (V6, V7 are contiguous)
    VST1 [V6.H8, V7.H8], (R0)

    RET


// func findMaxSIMD(src *[16]uint16) uint16
// Finds the maximum value among 16 uint16 values using ARM NEON.
// Uses VUMAX for unsigned max comparison.
TEXT ·findMaxSIMD(SB), NOSPLIT, $0-10
    MOVD src+0(FP), R0          // R0 = src pointer

    // Load all 16 values
    VLD1 (R0), [V0.H8, V1.H8]   // V0 = src[0:7], V1 = src[8:15]

    // Step 1: Max of V0 and V1 -> 8 values in V0
    VUMAX V1.H8, V0.H8, V0.H8   // V0 = max(V0, V1)

    // Step 2: Reduce 8 -> 4 values
    // Extract high 4 halfwords and compare with low 4
    VDUP V0.D[1], V1.D2         // V1 = broadcast high 64 bits
    VUMAX V1.H8, V0.H8, V0.H8   // V0[0:3] has max of all 8

    // Step 3: Reduce 4 -> 2 values
    VDUP V0.S[1], V1.S4         // V1 = broadcast second 32-bit element
    VUMAX V1.H8, V0.H8, V0.H8   // V0[0:1] has max of all 8

    // Step 4: Reduce 2 -> 1 value
    VDUP V0.H[1], V1.H8         // V1 = broadcast second 16-bit element
    VUMAX V1.H8, V0.H8, V0.H8   // V0[0] has max of all 16

    // Extract result to general register
    VMOV V0.H[0], R0
    MOVHU R0, ret+8(FP)

    RET


// func fromOrderedSIMD(dst, src *[16]uint16)
// Converts 16 values from ordered back to sign-magnitude representation.
// This is the inverse of toOrderedSIMD:
// - High bit set (ordered positive) -> clear high bit: v & 0x7FFF
// - High bit clear (ordered negative) -> NOT: ^v
TEXT ·fromOrderedSIMD(SB), NOSPLIT, $0-16
    MOVD dst+0(FP), R0          // R0 = dst pointer
    MOVD src+8(FP), R1          // R1 = src pointer

    // Load all 16 values
    VLD1 (R1), [V0.H8, V1.H8]   // V0 = src[0:7], V1 = src[8:15]

    // Create constants
    MOVD $0x8000800080008000, R2
    VDUP R2, V4.D2              // V4 = 0x8000 broadcast
    MOVD $0x7fff7fff7fff7fff, R2
    VDUP R2, V5.D2              // V5 = 0x7FFF broadcast

    // V20 = all 1s (for NOT operation via XOR)
    VMOVI $0xFF, V20.B16

    // Process first 8 values (V0)
    // ==============================

    // Compute positive result (high bit set in ordered): v & 0x7FFF
    VAND V5.B16, V0.B16, V6.B16     // V6 = v & 0x7FFF

    // Compute negative result (high bit clear in ordered): ^v
    VEOR V20.B16, V0.B16, V7.B16    // V7 = ^v

    // Create mask for high bit set: (v & 0x8000) == 0x8000
    VAND V4.B16, V0.B16, V8.B16     // V8 = v & 0x8000
    VCMEQ V4.H8, V8.H8, V8.H8       // V8 = 0xFFFF if high bit set (positive)

    // Blend: result = (mask & posResult) | (~mask & negResult)
    VAND V8.B16, V6.B16, V10.B16    // V10 = mask & posResult
    VEOR V20.B16, V8.B16, V11.B16   // V11 = ~mask
    VAND V11.B16, V7.B16, V11.B16   // V11 = negResult & ~mask
    VORR V10.B16, V11.B16, V6.B16   // V6 = final result for first 8

    // Process second 8 values (V1)
    // ==============================

    // Compute positive result: v & 0x7FFF
    VAND V5.B16, V1.B16, V12.B16    // V12 = v & 0x7FFF

    // Compute negative result: ^v
    VEOR V20.B16, V1.B16, V13.B16   // V13 = ^v

    // Create mask for high bit set
    VAND V4.B16, V1.B16, V14.B16    // V14 = v & 0x8000
    VCMEQ V4.H8, V14.H8, V14.H8     // V14 = 0xFFFF if high bit set

    // Blend
    VAND V14.B16, V12.B16, V10.B16  // V10 = mask & posResult
    VEOR V20.B16, V14.B16, V11.B16  // V11 = ~mask
    VAND V11.B16, V13.B16, V11.B16  // V11 = negResult & ~mask
    VORR V10.B16, V11.B16, V7.B16   // V7 = final result for second 8

    // Store results (V6, V7 are contiguous)
    VST1 [V6.H8, V7.H8], (R0)

    RET
