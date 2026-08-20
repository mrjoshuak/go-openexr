//go:build amd64

#include "textflag.h"

// func decodeASM(data []byte)
// Performs predictor decode (prefix sum) using SSE2 SIMD.
// Each byte becomes the sum of itself and all previous bytes.
TEXT ·decodeASM(SB), NOSPLIT, $0-24
    MOVQ data_base+0(FP), SI   // SI = &data[0]
    MOVQ data_len+8(FP), CX    // CX = len(data)

    CMPQ CX, $2
    JL   done                   // if len < 2, nothing to do

    CMPQ CX, $16
    JL   scalar                 // if len < 16, use scalar path

    // Initialize carry to zero
    PXOR X0, X0                 // X0 = carry (all zeros initially)

    // Calculate number of 16-byte chunks
    MOVQ CX, DX
    SHRQ $4, DX                 // DX = len / 16

    CMPQ DX, $0
    JE   remainder

simd_loop:
    // Load 16 bytes
    MOVOU (SI), X1              // X1 = data[i:i+16]

    // Parallel prefix sum within 16 bytes
    // Step 1: Add with 1-byte shift
    MOVOU X1, X2
    PSLLDQ $1, X2               // Shift left by 1 byte
    PADDB X2, X1                // X1[j] += X1[j-1] for j >= 1

    // Step 2: Add with 2-byte shift
    MOVOU X1, X2
    PSLLDQ $2, X2               // Shift left by 2 bytes
    PADDB X2, X1                // Now X1[j] = sum of X1[j-3:j+1]

    // Step 3: Add with 4-byte shift
    MOVOU X1, X2
    PSLLDQ $4, X2               // Shift left by 4 bytes
    PADDB X2, X1                // Now X1[j] = sum of X1[j-7:j+1]

    // Step 4: Add with 8-byte shift
    MOVOU X1, X2
    PSLLDQ $8, X2               // Shift left by 8 bytes
    PADDB X2, X1                // Now X1[j] = sum of X1[0:j+1]

    // Add carry from previous chunk (broadcast to all bytes)
    PADDB X0, X1

    // Store result
    MOVOU X1, (SI)

    // Extract last byte as new carry and broadcast it
    // We need to broadcast byte 15 to all positions
    MOVOU X1, X0
    PSRLDQ $15, X0              // Shift right to get last byte in position 0

    // Broadcast byte 0 to all positions using PUNPCKLBW + PSHUFD
    PUNPCKLBW X0, X0            // Duplicate bytes: 0,0,1,1,2,2,...
    PUNPCKLBW X0, X0            // Now: 0,0,0,0,1,1,1,1,...
    PSHUFD $0, X0, X0           // Broadcast lowest dword to all positions

    ADDQ $16, SI
    DECQ DX
    JNZ  simd_loop

remainder:
    // Handle remaining bytes (< 16)
    MOVQ CX, DX
    ANDQ $15, DX                // DX = len % 16
    CMPQ DX, $0
    JE   done

    // Get last processed byte value for carry
    // X0 already has the carry broadcast, extract byte 0
    MOVQ X0, AX                 // Get low 8 bytes of X0
    // AL now has the carry byte

    CMPQ DX, $0
    JE   done

remainder_loop:
    MOVB (SI), BL
    ADDB AL, BL
    MOVB BL, (SI)
    MOVB BL, AL                 // Update carry
    INCQ SI
    DECQ DX
    JNZ  remainder_loop
    JMP  done

scalar:
    // Scalar path for small arrays (len < 16)
    MOVB (SI), AL               // AL = data[0] (first byte, unchanged)
    INCQ SI
    DECQ CX                     // CX = len - 1

scalar_loop:
    CMPQ CX, $0
    JE   done
    MOVB (SI), BL
    ADDB AL, BL
    MOVB BL, (SI)
    MOVB BL, AL
    INCQ SI
    DECQ CX
    JMP  scalar_loop

done:
    RET
