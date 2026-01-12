// Package compression provides compression algorithms for OpenEXR files.
package compression

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
)

// Huffman coding for PIZ compression.
// OpenEXR uses a custom Huffman encoding for 16-bit values.

var (
	ErrHuffmanCorrupted   = errors.New("compression: corrupted Huffman data")
	ErrHuffmanOverflow    = errors.New("compression: Huffman decode overflow")
	ErrHuffmanInvalidCode = errors.New("compression: invalid Huffman code (table overflow)")
)

// huffmanCode represents a Huffman code
type huffmanCode struct {
	code   uint64
	length int
}

// huffmanNode is used for building the Huffman tree
type huffmanNode struct {
	symbol int
	count  uint64
	left   *huffmanNode
	right  *huffmanNode
}

// huffmanNodeArena pre-allocates nodes to reduce allocations during tree building.
type huffmanNodeArena struct {
	nodes []huffmanNode
	pos   int
}

// huffmanArenaPool reuses node arenas.
var huffmanArenaPool = sync.Pool{
	New: func() any {
		// Pre-allocate enough for max unique symbols + internal nodes
		// For N leaf nodes, we need at most N-1 internal nodes, so 2*N-1 total
		return &huffmanNodeArena{
			nodes: make([]huffmanNode, 65536*2),
		}
	},
}

func (a *huffmanNodeArena) reset() {
	a.pos = 0
}

func (a *huffmanNodeArena) alloc(symbol int, count uint64) *huffmanNode {
	if a.pos >= len(a.nodes) {
		// Fallback to heap allocation if we exceed capacity
		return &huffmanNode{symbol: symbol, count: count}
	}
	node := &a.nodes[a.pos]
	a.pos++
	node.symbol = symbol
	node.count = count
	node.left = nil
	node.right = nil
	return node
}

// huffmanHeap implements heap.Interface for huffmanNode (min-heap by count)
type huffmanHeap []*huffmanNode

func (h huffmanHeap) Len() int           { return len(h) }
func (h huffmanHeap) Less(i, j int) bool { return h[i].count < h[j].count }
func (h huffmanHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *huffmanHeap) Push(x any) {
	*h = append(*h, x.(*huffmanNode))
}

func (h *huffmanHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return x
}

// HuffmanEncoder encodes 16-bit values using Huffman coding
type HuffmanEncoder struct {
	codes   []huffmanCode
	lengths []int // for serialization
}

// NewHuffmanEncoder creates a new Huffman encoder from frequency counts
func NewHuffmanEncoder(freqs []uint64) *HuffmanEncoder {
	if len(freqs) == 0 {
		return &HuffmanEncoder{codes: make([]huffmanCode, 0), lengths: make([]int, 0)}
	}

	// Get a node arena from the pool
	arena := huffmanArenaPool.Get().(*huffmanNodeArena)
	arena.reset()
	defer huffmanArenaPool.Put(arena)

	// Build nodes for all symbols with non-zero frequency
	nodes := make(huffmanHeap, 0, len(freqs))
	for symbol, count := range freqs {
		if count > 0 {
			nodes = append(nodes, arena.alloc(symbol, count))
		}
	}

	codes := make([]huffmanCode, len(freqs))
	lengths := make([]int, len(freqs))

	if len(nodes) == 0 {
		return &HuffmanEncoder{codes: codes, lengths: lengths}
	}

	if len(nodes) == 1 {
		// Single symbol - assign code 0 with length 1
		codes[nodes[0].symbol] = huffmanCode{code: 0, length: 1}
		lengths[nodes[0].symbol] = 1
		return &HuffmanEncoder{codes: codes, lengths: lengths}
	}

	// Build Huffman tree using min-heap (O(n log n) instead of O(n² log n))
	heap.Init(&nodes)
	for nodes.Len() > 1 {
		// Pop two smallest
		left := heap.Pop(&nodes).(*huffmanNode)
		right := heap.Pop(&nodes).(*huffmanNode)
		// Create parent using arena
		parent := arena.alloc(-1, left.count+right.count)
		parent.left = left
		parent.right = right
		heap.Push(&nodes, parent)
	}

	// Compute code lengths from tree
	if nodes.Len() > 0 {
		computeLengths(nodes[0], 0, lengths)
	}

	// Generate canonical Huffman codes from lengths
	generateCanonicalCodes(codes, lengths)

	return &HuffmanEncoder{codes: codes, lengths: lengths}
}

// computeLengths computes code lengths by traversing the tree
func computeLengths(node *huffmanNode, depth int, lengths []int) {
	if node == nil {
		return
	}

	if node.left == nil && node.right == nil {
		// Leaf node
		if node.symbol >= 0 && node.symbol < len(lengths) {
			lengths[node.symbol] = depth
		}
		return
	}

	// Internal node
	computeLengths(node.left, depth+1, lengths)
	computeLengths(node.right, depth+1, lengths)
}

// generateCanonicalCodes generates canonical Huffman codes from lengths
func generateCanonicalCodes(codes []huffmanCode, lengths []int) {
	maxLen := 0
	for _, l := range lengths {
		if l > maxLen {
			maxLen = l
		}
	}

	if maxLen == 0 {
		return
	}

	// Count symbols per length
	lengthCount := make([]int, maxLen+1)
	for _, length := range lengths {
		if length > 0 {
			lengthCount[length]++
		}
	}

	// Calculate starting codes for each length
	code := uint64(0)
	nextCode := make([]uint64, maxLen+1)
	for bits := 1; bits <= maxLen; bits++ {
		code = (code + uint64(lengthCount[bits-1])) << 1
		nextCode[bits] = code
	}

	// Assign codes to symbols in order
	for symbol, length := range lengths {
		if length > 0 {
			codes[symbol] = huffmanCode{code: nextCode[length], length: length}
			nextCode[length]++
		}
	}
}

// Encode encodes a slice of 16-bit values
func (e *HuffmanEncoder) Encode(values []uint16) []byte {
	if len(values) == 0 || len(e.codes) == 0 {
		return nil
	}

	// Calculate output size estimate
	var totalBits int
	for _, v := range values {
		if int(v) < len(e.codes) {
			totalBits += e.codes[v].length
		}
	}

	if totalBits == 0 {
		return nil
	}

	result := make([]byte, (totalBits+7)/8)

	// Use a bit buffer for efficient writing
	var bitBuffer uint64
	var bitsInBuffer int
	resultPos := 0

	for _, v := range values {
		if int(v) >= len(e.codes) {
			continue
		}
		c := e.codes[v]
		if c.length == 0 {
			continue
		}

		// Add code to bit buffer (MSB first)
		bitBuffer = (bitBuffer << uint(c.length)) | c.code
		bitsInBuffer += c.length

		// Flush complete bytes
		for bitsInBuffer >= 8 {
			bitsInBuffer -= 8
			result[resultPos] = byte(bitBuffer >> uint(bitsInBuffer))
			resultPos++
		}
	}

	// Flush remaining bits
	if bitsInBuffer > 0 {
		result[resultPos] = byte(bitBuffer << uint(8-bitsInBuffer))
	}

	return result
}

// GetCodes returns the code table for serialization
func (e *HuffmanEncoder) GetCodes() []huffmanCode {
	return e.codes
}

// GetLengths returns the code lengths for serialization
func (e *HuffmanEncoder) GetLengths() []int {
	return e.lengths
}

// HuffmanDecoder decodes Huffman-encoded data
type HuffmanDecoder struct {
	// Fast lookup table for codes up to tableBits
	table     []int32 // symbol (>=0) or -1 if not found
	tableBits int
	// Fallback map for longer codes: (code << 6 | length) -> symbol
	longCodes map[uint64]int
	maxLen    int
}

const huffmanTableBits = 12 // Direct lookup for codes up to 12 bits (16KB table fits in L1 cache)

// FastHufDecoder is an optimized Huffman decoder based on OpenEXR's FastHufDecoder.
// Key optimizations over HuffmanDecoder:
// - Left-justified 64-bit bit buffers for direct comparison
// - Double buffering to hide memory latency
// - 64-bit block reads instead of byte-at-a-time
// - Separate symbol and length tables for cache efficiency
// - Two-level lookup: 14-bit primary table + sorted binary search for longer codes
type FastHufDecoder struct {
	// Separate tables for symbols and lengths (cache-friendly)
	tableSymbol  []uint16 // 32KB at 14 bits
	tableCodeLen []uint8  // 16KB at 14 bits
	tableBits    int
	tableSize    int

	// For codes longer than tableBits, sorted by left-justified code for binary search
	longCodes       []longHufCode
	longCodesSorted bool // true if longCodes is sorted for binary search
	maxLen          int

	// Threshold for table lookup (minimum valid left-justified code)
	tableMin uint64

	// Pre-allocated buffers for Reset (avoid allocations)
	codes       []huffmanCode // Reusable buffer for canonical codes
	lengthCount []int         // Reusable buffer for length counting
	nextCode    []uint64      // Reusable buffer for next code per length
	maxTableIdx int           // Track max table index used for faster clearing
}

// longHufCode stores codes that don't fit in the fast lookup table
type longHufCode struct {
	code   uint64 // Left-justified code
	symbol uint32 // Can be up to 65536 (RLE symbol)
	length uint8
}

const (
	fastHufTableBits  = 14                    // Increased from 12 to 14 bits
	fastHufIndexShift = 64 - fastHufTableBits // 50 bits
	fastHufTableSize  = 1 << fastHufTableBits // 16384 entries
)

// fastHufDecoderPool pools FastHufDecoder instances to avoid repeated allocations.
// Each decoder has ~48KB of lookup tables that can be reused.
var fastHufDecoderPool = sync.Pool{
	New: func() any {
		return &FastHufDecoder{
			tableSymbol:  make([]uint16, fastHufTableSize),
			tableCodeLen: make([]uint8, fastHufTableSize),
			tableBits:    fastHufTableBits,
			tableSize:    fastHufTableSize,
			longCodes:    make([]longHufCode, 0, 64), // Pre-allocate some capacity
			codes:        make([]huffmanCode, 65537), // Max symbols (65536 + RLE)
			lengthCount:  make([]int, 64),            // Max code length
			nextCode:     make([]uint64, 64),         // Max code length
		}
	},
}

// GetFastHufDecoder gets a decoder from the pool and initializes it with the given code lengths.
// Call PutFastHufDecoder when done to return it to the pool.
func GetFastHufDecoder(codeLengths []int) (*FastHufDecoder, error) {
	d := fastHufDecoderPool.Get().(*FastHufDecoder)
	if err := d.Reset(codeLengths); err != nil {
		fastHufDecoderPool.Put(d)
		return nil, err
	}
	return d, nil
}

// GetFastHufDecoderWithBounds gets a decoder from the pool and initializes it with code lengths,
// only examining the range [minIdx, maxIdx]. This is much faster when most code lengths are zero.
func GetFastHufDecoderWithBounds(codeLengths []int, minIdx, maxIdx int) (*FastHufDecoder, error) {
	d := fastHufDecoderPool.Get().(*FastHufDecoder)
	if err := d.ResetWithBounds(codeLengths, minIdx, maxIdx); err != nil {
		fastHufDecoderPool.Put(d)
		return nil, err
	}
	return d, nil
}

// PutFastHufDecoder returns a decoder to the pool for reuse.
func PutFastHufDecoder(d *FastHufDecoder) {
	if d == nil {
		return
	}
	fastHufDecoderPool.Put(d)
}

// Reset reinitializes the decoder with new code lengths, reusing allocated memory.
// Returns an error if the code lengths would produce invalid table indices.
func (d *FastHufDecoder) Reset(codeLengths []int) error {
	// Clear only the used portion of the table (much faster than clearing all 16K entries)
	if d.maxTableIdx > 0 {
		for i := 0; i <= d.maxTableIdx; i++ {
			d.tableCodeLen[i] = 0
		}
	}
	d.longCodes = d.longCodes[:0]
	d.tableMin = ^uint64(0)
	d.maxTableIdx = 0

	maxLen := 0
	for _, l := range codeLengths {
		if l > maxLen {
			maxLen = l
		}
	}

	d.maxLen = maxLen
	if maxLen == 0 {
		return nil
	}

	// Use pre-allocated buffers for canonical code generation
	numSymbols := len(codeLengths)
	codes := d.codes
	if len(codes) < numSymbols {
		codes = make([]huffmanCode, numSymbols)
		d.codes = codes
	}
	// Clear the portion we'll use
	for i := 0; i < numSymbols; i++ {
		codes[i] = huffmanCode{}
	}

	// Generate canonical codes using pre-allocated buffers
	d.generateCanonicalCodesReuse(codes[:numSymbols], codeLengths, maxLen)

	// Build separate symbol and length tables
	for symbol, c := range codes[:numSymbols] {
		if c.length == 0 {
			continue
		}

		// Convert to left-justified representation
		leftJustified := c.code << (64 - c.length)

		if c.length <= fastHufTableBits {
			// Fast table path: fill all entries with matching prefix
			shift := fastHufTableBits - c.length
			base := int(c.code) << shift
			count := 1 << shift
			endIdx := base + count - 1

			// Bounds check: ensure we don't write outside the table
			if endIdx >= fastHufTableSize {
				return ErrHuffmanInvalidCode
			}

			for i := 0; i < count; i++ {
				idx := base + i
				d.tableSymbol[idx] = uint16(symbol)
				d.tableCodeLen[idx] = uint8(c.length)
			}

			// Track max index for faster clearing next time
			if endIdx > d.maxTableIdx {
				d.maxTableIdx = endIdx
			}

			if leftJustified < d.tableMin {
				d.tableMin = leftJustified
			}
		} else {
			d.longCodes = append(d.longCodes, longHufCode{
				code:   leftJustified,
				symbol: uint32(symbol),
				length: uint8(c.length),
			})
		}
	}

	// Sort longCodes by left-justified code value for binary search
	if len(d.longCodes) > 1 {
		sort.Slice(d.longCodes, func(i, j int) bool {
			return d.longCodes[i].code < d.longCodes[j].code
		})
	}
	d.longCodesSorted = true
	return nil
}

// ResetWithBounds reinitializes the decoder with new code lengths, only examining [minIdx, maxIdx].
// This is much faster than Reset when most code lengths are zero (typical for PIZ).
// Returns an error if the code lengths would produce invalid table indices.
func (d *FastHufDecoder) ResetWithBounds(codeLengths []int, minIdx, maxIdx int) error {
	// Clear only the used portion of the table
	if d.maxTableIdx > 0 {
		for i := 0; i <= d.maxTableIdx; i++ {
			d.tableCodeLen[i] = 0
		}
	}
	d.longCodes = d.longCodes[:0]
	d.tableMin = ^uint64(0)
	d.maxTableIdx = 0

	// Bounds check
	if minIdx < 0 {
		minIdx = 0
	}
	if maxIdx >= len(codeLengths) {
		maxIdx = len(codeLengths) - 1
	}
	if minIdx > maxIdx {
		d.maxLen = 0
		return nil
	}

	// Find maxLen only in the valid range
	maxLen := 0
	for i := minIdx; i <= maxIdx; i++ {
		if codeLengths[i] > maxLen {
			maxLen = codeLengths[i]
		}
	}

	d.maxLen = maxLen
	if maxLen == 0 {
		return nil
	}

	// Use pre-allocated lengthCount buffer
	lengthCount := d.lengthCount
	if len(lengthCount) <= maxLen {
		lengthCount = make([]int, maxLen+1)
		d.lengthCount = lengthCount
	}
	for i := 0; i <= maxLen; i++ {
		lengthCount[i] = 0
	}

	// Count symbols per length (only in valid range)
	for i := minIdx; i <= maxIdx; i++ {
		length := codeLengths[i]
		if length > 0 {
			lengthCount[length]++
		}
	}

	// Use pre-allocated nextCode buffer
	nextCode := d.nextCode
	if len(nextCode) <= maxLen {
		nextCode = make([]uint64, maxLen+1)
		d.nextCode = nextCode
	}

	// Calculate starting codes for each length
	code := uint64(0)
	for bits := 1; bits <= maxLen; bits++ {
		code = (code + uint64(lengthCount[bits-1])) << 1
		nextCode[bits] = code
	}

	// Build tables - only process symbols in valid range
	for symbol := minIdx; symbol <= maxIdx; symbol++ {
		length := codeLengths[symbol]
		if length == 0 {
			continue
		}

		symbolCode := nextCode[length]
		nextCode[length]++

		// Convert to left-justified representation
		leftJustified := symbolCode << (64 - length)

		if length <= fastHufTableBits {
			// Fast table path: fill all entries with matching prefix
			shift := fastHufTableBits - length
			base := int(symbolCode) << shift
			count := 1 << shift
			endIdx := base + count - 1

			// Bounds check: ensure we don't write outside the table
			if endIdx >= fastHufTableSize {
				return ErrHuffmanInvalidCode
			}

			for i := 0; i < count; i++ {
				idx := base + i
				d.tableSymbol[idx] = uint16(symbol)
				d.tableCodeLen[idx] = uint8(length)
			}

			if endIdx > d.maxTableIdx {
				d.maxTableIdx = endIdx
			}

			if leftJustified < d.tableMin {
				d.tableMin = leftJustified
			}
		} else {
			d.longCodes = append(d.longCodes, longHufCode{
				code:   leftJustified,
				symbol: uint32(symbol),
				length: uint8(length),
			})
		}
	}

	// Sort longCodes by left-justified code value for binary search
	if len(d.longCodes) > 1 {
		sort.Slice(d.longCodes, func(i, j int) bool {
			return d.longCodes[i].code < d.longCodes[j].code
		})
	}
	d.longCodesSorted = true
	return nil
}

// generateCanonicalCodesReuse generates canonical Huffman codes using pre-allocated buffers.
func (d *FastHufDecoder) generateCanonicalCodesReuse(codes []huffmanCode, lengths []int, maxLen int) {
	// Use pre-allocated lengthCount buffer
	lengthCount := d.lengthCount
	if len(lengthCount) <= maxLen {
		lengthCount = make([]int, maxLen+1)
		d.lengthCount = lengthCount
	}
	// Clear the portion we'll use
	for i := 0; i <= maxLen; i++ {
		lengthCount[i] = 0
	}

	// Count symbols per length
	for _, length := range lengths {
		if length > 0 {
			lengthCount[length]++
		}
	}

	// Use pre-allocated nextCode buffer
	nextCode := d.nextCode
	if len(nextCode) <= maxLen {
		nextCode = make([]uint64, maxLen+1)
		d.nextCode = nextCode
	}

	// Calculate starting codes for each length
	code := uint64(0)
	for bits := 1; bits <= maxLen; bits++ {
		code = (code + uint64(lengthCount[bits-1])) << 1
		nextCode[bits] = code
	}

	// Assign codes to symbols in order
	for symbol, length := range lengths {
		if length > 0 {
			codes[symbol] = huffmanCode{code: nextCode[length], length: length}
			nextCode[length]++
		}
	}
}

// NewHuffmanDecoder creates a decoder from code lengths
func NewHuffmanDecoder(codeLengths []int) *HuffmanDecoder {
	codes := make([]huffmanCode, len(codeLengths))
	maxLen := 0

	for _, l := range codeLengths {
		if l > maxLen {
			maxLen = l
		}
	}

	generateCanonicalCodes(codes, codeLengths)

	d := &HuffmanDecoder{
		maxLen:    maxLen,
		longCodes: make(map[uint64]int),
	}

	// Build fast lookup table for short codes
	if maxLen > 0 {
		tableBits := huffmanTableBits
		if maxLen < tableBits {
			tableBits = maxLen
		}
		d.tableBits = tableBits
		d.table = make([]int32, 1<<tableBits)
		for i := range d.table {
			d.table[i] = -1 // Mark as invalid
		}

		// Fill table with all codes
		for symbol, c := range codes {
			if c.length > 0 {
				if c.length <= tableBits {
					// This code fits in the table
					// Fill all entries that start with this code
					shift := tableBits - c.length
					base := int(c.code) << shift
					for i := 0; i < (1 << shift); i++ {
						// Store symbol and length: (length << 16) | symbol
						d.table[base+i] = int32((c.length << 16) | symbol)
					}
				} else {
					// Long code - store in map
					key := (c.code << 6) | uint64(c.length)
					d.longCodes[key] = symbol
				}
			}
		}
	}

	return d
}

// Decode decodes Huffman-encoded data
func (d *HuffmanDecoder) Decode(data []byte, numValues int) ([]uint16, error) {
	if len(data) == 0 || numValues == 0 {
		return nil, nil
	}

	if d.maxLen == 0 {
		return nil, ErrHuffmanCorrupted
	}

	result := make([]uint16, numValues)
	resultIdx := 0

	// Bit buffer for fast reading - bits are stored MSB first
	var bitBuffer uint64
	var bitsInBuffer int
	dataPos := 0
	dataLen := len(data)

	// Cache table access values
	tableBits := d.tableBits
	tableMask := uint64((1 << tableBits) - 1)
	table := d.table

	// Fill initial buffer (up to 56 bits to leave room for more)
	for bitsInBuffer <= 56 && dataPos < dataLen {
		bitBuffer = (bitBuffer << 8) | uint64(data[dataPos])
		dataPos++
		bitsInBuffer += 8
	}

	// Main decode loop - optimized for table hits
	for resultIdx < numValues {
		// Refill buffer if we can (keep at least tableBits + some headroom)
		for bitsInBuffer <= 56 && dataPos < dataLen {
			bitBuffer = (bitBuffer << 8) | uint64(data[dataPos])
			dataPos++
			bitsInBuffer += 8
		}

		if bitsInBuffer < 1 {
			return nil, ErrHuffmanCorrupted
		}

		// Fast table lookup - most codes should hit here
		if bitsInBuffer >= tableBits {
			idx := int((bitBuffer >> (bitsInBuffer - tableBits)) & tableMask)
			entry := table[idx]
			if entry >= 0 {
				length := int(entry >> 16)
				symbol := int(entry & 0xFFFF)
				result[resultIdx] = uint16(symbol)
				resultIdx++
				bitsInBuffer -= length
				continue
			}
		}

		// Fallback path for edge cases
		found := false

		// For remaining bits < tableBits, scan table entries that match
		if bitsInBuffer < tableBits && bitsInBuffer > 0 {
			for length := 1; length <= bitsInBuffer && length <= d.maxLen; length++ {
				code := (bitBuffer >> (bitsInBuffer - length)) & ((1 << length) - 1)
				if length <= tableBits {
					shift := tableBits - length
					idx := int(code << shift)
					if idx < len(table) {
						entry := table[idx]
						if entry >= 0 && int(entry>>16) == length {
							symbol := int(entry & 0xFFFF)
							result[resultIdx] = uint16(symbol)
							resultIdx++
							bitsInBuffer -= length
							found = true
							break
						}
					}
				}
			}
		}

		// Long codes: use map lookup
		if !found && len(d.longCodes) > 0 {
			startLen := tableBits + 1
			if startLen < 1 {
				startLen = 1
			}
			for length := startLen; length <= d.maxLen && length <= bitsInBuffer; length++ {
				code := (bitBuffer >> (bitsInBuffer - length)) & ((1 << length) - 1)
				key := (code << 6) | uint64(length)
				if symbol, ok := d.longCodes[key]; ok {
					result[resultIdx] = uint16(symbol)
					resultIdx++
					bitsInBuffer -= length
					found = true
					break
				}
			}
		}

		if !found {
			return nil, ErrHuffmanCorrupted
		}
	}

	return result, nil
}

// NewFastHufDecoder creates an optimized decoder from code lengths.
// This uses the FastHufDecoder architecture for better performance.
func NewFastHufDecoder(codeLengths []int) *FastHufDecoder {
	codes := make([]huffmanCode, len(codeLengths))
	maxLen := 0

	for _, l := range codeLengths {
		if l > maxLen {
			maxLen = l
		}
	}

	if maxLen == 0 {
		return &FastHufDecoder{
			tableSymbol:  make([]uint16, fastHufTableSize),
			tableCodeLen: make([]uint8, fastHufTableSize),
			tableBits:    fastHufTableBits,
			tableSize:    fastHufTableSize,
		}
	}

	generateCanonicalCodes(codes, codeLengths)

	d := &FastHufDecoder{
		tableBits: fastHufTableBits,
		tableSize: fastHufTableSize,
		maxLen:    maxLen,
		tableMin:  ^uint64(0), // Will find minimum valid code
	}

	d.tableSymbol = make([]uint16, fastHufTableSize)
	d.tableCodeLen = make([]uint8, fastHufTableSize)

	// Build separate symbol and length tables
	for symbol, c := range codes {
		if c.length == 0 {
			continue
		}

		// Convert to left-justified representation
		// Code stored at the MSB of a 64-bit value
		leftJustified := c.code << (64 - c.length)

		if c.length <= fastHufTableBits {
			// Fast table path: fill all entries with matching prefix
			shift := fastHufTableBits - c.length
			base := int(c.code) << shift
			count := 1 << shift

			for i := 0; i < count; i++ {
				idx := base + i
				d.tableSymbol[idx] = uint16(symbol)
				d.tableCodeLen[idx] = uint8(c.length)
			}

			// Track minimum left-justified code for table lookup threshold
			if leftJustified < d.tableMin {
				d.tableMin = leftJustified
			}
		} else {
			// Long code: store for binary search
			d.longCodes = append(d.longCodes, longHufCode{
				code:   leftJustified,
				symbol: uint32(symbol),
				length: uint8(c.length),
			})
		}
	}

	// Sort longCodes by left-justified code value for binary search
	if len(d.longCodes) > 1 {
		sort.Slice(d.longCodes, func(i, j int) bool {
			return d.longCodes[i].code < d.longCodes[j].code
		})
	}
	d.longCodesSorted = true

	return d
}

// Decode decodes Huffman-encoded data using the FastHufDecoder.
// This implementation uses left-justified bit buffers, double buffering, and separate symbol/length tables.
func (d *FastHufDecoder) Decode(data []byte, numValues int) ([]uint16, error) {
	result := make([]uint16, numValues)
	if err := d.DecodeInto(data, result); err != nil {
		return nil, err
	}
	return result, nil
}

// DecodeInto decodes Huffman-encoded data into a pre-allocated buffer.
// The result buffer must have capacity for numValues elements.
func (d *FastHufDecoder) DecodeInto(data []byte, result []uint16) error {
	numValues := len(result)
	if len(data) == 0 || numValues == 0 {
		return nil
	}

	if d.maxLen == 0 {
		return ErrHuffmanCorrupted
	}
	resultIdx := 0
	dataLen := len(data)

	// Double-buffered left-justified bit buffers
	// buffer: current working buffer with valid bits at MSB
	// bufferBack: pre-loaded next 64 bits, left-justified
	var buffer, bufferBack uint64
	var bitsInBuffer, bitsInBack int
	dataPos := 0

	// Cache frequently used values
	tableSymbol := d.tableSymbol
	tableCodeLen := d.tableCodeLen
	tableMin := d.tableMin
	longCodes := d.longCodes

	// read64 reads up to 8 bytes as a big-endian uint64, left-justified
	// Returns the value and number of bits read
	read64 := func() (uint64, int) {
		remaining := dataLen - dataPos
		if remaining >= 8 {
			v := binary.BigEndian.Uint64(data[dataPos:])
			dataPos += 8
			return v, 64
		}
		if remaining <= 0 {
			return 0, 0
		}
		// Handle tail bytes
		var v uint64
		for i := 0; i < remaining; i++ {
			v = (v << 8) | uint64(data[dataPos+i])
		}
		dataPos = dataLen
		// Left-justify
		return v << ((8 - remaining) * 8), remaining * 8
	}

	// Initial fill - both buffers
	buffer, bitsInBuffer = read64()
	bufferBack, bitsInBack = read64()

	// Main decode loop
	for resultIdx < numValues {
		// Refill from bufferBack when we've consumed enough bits
		if bitsInBuffer <= 32 && bitsInBack > 0 {
			// Transfer bits from bufferBack to buffer
			toTransfer := 64 - bitsInBuffer
			if toTransfer > bitsInBack {
				toTransfer = bitsInBack
			}
			buffer |= bufferBack >> bitsInBuffer
			bitsInBuffer += toTransfer
			bufferBack <<= toTransfer
			bitsInBack -= toTransfer

			// Refill bufferBack if empty
			if bitsInBack == 0 && dataPos < dataLen {
				bufferBack, bitsInBack = read64()
			}
		}

		if bitsInBuffer < 1 {
			return ErrHuffmanCorrupted
		}

		// Fast path: table lookup for codes <= 12 bits
		// Buffer is left-justified, so top 12 bits are the table index
		if buffer >= tableMin {
			idx := buffer >> fastHufIndexShift
			codeLen := int(tableCodeLen[idx])
			if codeLen > 0 && codeLen <= bitsInBuffer {
				result[resultIdx] = tableSymbol[idx]
				resultIdx++
				buffer <<= codeLen
				bitsInBuffer -= codeLen
				continue
			}
		}

		// Slow path: binary search long codes (sorted by left-justified code value)
		found := false
		if len(longCodes) > 0 {
			// Binary search to find insertion point
			n := len(longCodes)
			lo, hi := 0, n
			for lo < hi {
				mid := lo + (hi-lo)/2
				if longCodes[mid].code <= buffer {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			// lo is now the first index where code > buffer
			// Check codes from lo-1 backwards (codes <= buffer)
			for i := lo - 1; i >= 0; i-- {
				lc := &longCodes[i]
				codeLen := int(lc.length)
				if codeLen > bitsInBuffer {
					continue
				}
				mask := ^uint64(0) << (64 - codeLen)
				maskedBuffer := buffer & mask
				if maskedBuffer == lc.code {
					result[resultIdx] = uint16(lc.symbol)
					resultIdx++
					buffer <<= codeLen
					bitsInBuffer -= codeLen
					found = true
					break
				}
				if maskedBuffer > lc.code {
					break
				}
			}
			// Also check forward in case of equal prefix
			if !found {
				for i := lo; i < n; i++ {
					lc := &longCodes[i]
					codeLen := int(lc.length)
					if codeLen > bitsInBuffer {
						continue
					}
					mask := ^uint64(0) << (64 - codeLen)
					maskedBuffer := buffer & mask
					if maskedBuffer == lc.code {
						result[resultIdx] = uint16(lc.symbol)
						resultIdx++
						buffer <<= codeLen
						bitsInBuffer -= codeLen
						found = true
						break
					}
					if lc.code > maskedBuffer {
						break
					}
				}
			}
		}

		if !found {
			// Try table lookup for remaining bits at end of stream
			if bitsInBuffer > 0 && bitsInBuffer <= fastHufTableBits {
				idx := buffer >> fastHufIndexShift
				codeLen := int(tableCodeLen[idx])
				if codeLen > 0 && codeLen <= bitsInBuffer {
					result[resultIdx] = tableSymbol[idx]
					resultIdx++
					buffer <<= codeLen
					bitsInBuffer -= codeLen
					continue
				}
			}
			return ErrHuffmanCorrupted
		}
	}

	return nil
}

// debugHuf enables debug output for Huffman decoding
var debugHuf = false

// DecodeIntoWithBits decodes Huffman-encoded data into a pre-allocated buffer.
// Uses nBits to determine exact bit count and handles RLE with rleSymbol.
// This matches the OpenEXR hufDecode function.
func (d *FastHufDecoder) DecodeIntoWithBits(data []byte, result []uint16, nBits int, rleSymbol int) error {
	numValues := len(result)
	if len(data) == 0 || numValues == 0 {
		return nil
	}

	if d.maxLen == 0 {
		if debugHuf {
			println("HufDecodeWithBits: maxLen is 0!")
		}
		return ErrHuffmanCorrupted
	}

	if debugHuf {
		println("HufDecodeWithBits: data len=", len(data), "numValues=", numValues, "nBits=", nBits, "rleSymbol=", rleSymbol)
		println("  maxLen=", d.maxLen, "tableMin=", d.tableMin, "longCodes len=", len(d.longCodes))
		// Check if RLE symbol is in longCodes
		rleInLongCodes := false
		for _, lc := range d.longCodes {
			if int(lc.symbol) == rleSymbol {
				rleInLongCodes = true
				println("  RLE symbol found in longCodes: code=", lc.code, "length=", lc.length)
				break
			}
		}
		if !rleInLongCodes {
			println("  WARNING: RLE symbol NOT in longCodes!")
		}
	}
	resultIdx := 0
	dataLen := len(data)
	bitsRemaining := nBits // Track bits consumed to know when to stop

	// Double-buffered left-justified bit buffers
	var buffer, bufferBack uint64
	var bitsInBuffer, bitsInBack int
	dataPos := 0

	// Cache frequently used values
	tableSymbol := d.tableSymbol
	tableCodeLen := d.tableCodeLen
	tableMin := d.tableMin
	longCodes := d.longCodes

	// read64 reads up to 8 bytes as a big-endian uint64, left-justified
	read64 := func() (uint64, int) {
		remaining := dataLen - dataPos
		if remaining >= 8 {
			v := binary.BigEndian.Uint64(data[dataPos:])
			dataPos += 8
			return v, 64
		}
		if remaining <= 0 {
			return 0, 0
		}
		var v uint64
		for i := 0; i < remaining; i++ {
			v = (v << 8) | uint64(data[dataPos+i])
		}
		dataPos = dataLen
		return v << ((8 - remaining) * 8), remaining * 8
	}

	// Initial fill - both buffers
	buffer, bitsInBuffer = read64()
	bufferBack, bitsInBack = read64()

	// Main decode loop - stop when we've consumed nBits or filled result
	for resultIdx < numValues && bitsRemaining > 0 {
		// Refill from bufferBack when we've consumed enough bits
		if bitsInBuffer <= 32 && bitsInBack > 0 {
			toTransfer := 64 - bitsInBuffer
			if toTransfer > bitsInBack {
				toTransfer = bitsInBack
			}
			buffer |= bufferBack >> bitsInBuffer
			bitsInBuffer += toTransfer
			bufferBack <<= toTransfer
			bitsInBack -= toTransfer

			if bitsInBack == 0 && dataPos < dataLen {
				bufferBack, bitsInBack = read64()
			}
		}

		if bitsInBuffer < 1 {
			break // End of data
		}

		var symbol uint32 // uint32 to handle RLE symbol (65536)
		var codeLen int

		// Fast path: table lookup for codes <= 14 bits
		foundCode := false
		if buffer >= tableMin {
			idx := buffer >> fastHufIndexShift
			codeLen = int(tableCodeLen[idx])
			if codeLen > 0 && codeLen <= bitsInBuffer && codeLen <= bitsRemaining {
				symbol = uint32(tableSymbol[idx])
				buffer <<= codeLen
				bitsInBuffer -= codeLen
				bitsRemaining -= codeLen
				foundCode = true
			} else {
				codeLen = 0
			}
		}

		// Slow path: binary search long codes (sorted by left-justified code value)
		if !foundCode && len(longCodes) > 0 {
			// Binary search to find insertion point
			// The matching code (if any) will be at or before this point
			n := len(longCodes)
			lo, hi := 0, n
			for lo < hi {
				mid := lo + (hi-lo)/2
				if longCodes[mid].code <= buffer {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			// lo is now the first index where code > buffer
			// Check codes from lo-1 backwards (codes <= buffer)
			for i := lo - 1; i >= 0; i-- {
				lc := &longCodes[i]
				codeLen = int(lc.length)
				if codeLen > bitsInBuffer || codeLen > bitsRemaining {
					continue
				}
				mask := ^uint64(0) << (64 - codeLen)
				maskedBuffer := buffer & mask
				if maskedBuffer == lc.code {
					symbol = lc.symbol
					buffer <<= codeLen
					bitsInBuffer -= codeLen
					bitsRemaining -= codeLen
					foundCode = true
					break
				}
				// If masked buffer is greater than code, no earlier codes can match
				if maskedBuffer > lc.code {
					break
				}
			}
			// Also check forward in case of equal prefix
			if !foundCode {
				for i := lo; i < n; i++ {
					lc := &longCodes[i]
					codeLen = int(lc.length)
					if codeLen > bitsInBuffer || codeLen > bitsRemaining {
						continue
					}
					mask := ^uint64(0) << (64 - codeLen)
					maskedBuffer := buffer & mask
					if maskedBuffer == lc.code {
						symbol = lc.symbol
						buffer <<= codeLen
						bitsInBuffer -= codeLen
						bitsRemaining -= codeLen
						foundCode = true
						break
					}
					// If code is greater than masked buffer, no later codes can match
					if lc.code > maskedBuffer {
						break
					}
				}
			}
		}

		if !foundCode {
			// Try table lookup for remaining bits at end of stream
			if bitsInBuffer > 0 && bitsInBuffer <= fastHufTableBits {
				idx := buffer >> fastHufIndexShift
				codeLen = int(tableCodeLen[idx])
				if codeLen > 0 && codeLen <= bitsInBuffer && codeLen <= bitsRemaining {
					symbol = uint32(tableSymbol[idx])
					buffer <<= codeLen
					bitsInBuffer -= codeLen
					bitsRemaining -= codeLen
					foundCode = true
				}
			}
			if !foundCode {
				// If remaining bits are less than minimum possible code, we're done
				// (padding bits at end of stream)
				break
			}
		}

		// Handle RLE symbol
		if debugHuf && foundCode && int(symbol) == rleSymbol {
			println("  Found RLE symbol at resultIdx=", resultIdx)
		}
		if int(symbol) == rleSymbol {
			// Refill if needed to read 8-bit RLE count
			if bitsInBuffer < 8 && bitsInBack > 0 {
				toTransfer := 64 - bitsInBuffer
				if toTransfer > bitsInBack {
					toTransfer = bitsInBack
				}
				buffer |= bufferBack >> bitsInBuffer
				bitsInBuffer += toTransfer
				bufferBack <<= toTransfer
				bitsInBack -= toTransfer

				if bitsInBack == 0 && dataPos < dataLen {
					bufferBack, bitsInBack = read64()
				}
			}

			if bitsInBuffer < 8 || bitsRemaining < 8 {
				break // Not enough bits for RLE count
			}

			// Read 8-bit run count
			runCount := int(buffer >> 56)
			buffer <<= 8
			bitsInBuffer -= 8
			bitsRemaining -= 8

			if debugHuf {
				println("  RLE: runCount=", runCount, "at resultIdx=", resultIdx)
			}

			if resultIdx < 1 {
				return ErrHuffmanCorrupted
			}

			if resultIdx+runCount > numValues {
				runCount = numValues - resultIdx // Clamp to remaining space
			}

			// Repeat previous symbol
			prevSymbol := result[resultIdx-1]
			for i := 0; i < runCount; i++ {
				result[resultIdx] = prevSymbol
				resultIdx++
			}
		} else {
			result[resultIdx] = uint16(symbol)
			resultIdx++
		}
	}

	if debugHuf {
		println("  Decode finished: resultIdx=", resultIdx, "numValues=", numValues, "bitsRemaining=", bitsRemaining)
	}

	return nil
}
