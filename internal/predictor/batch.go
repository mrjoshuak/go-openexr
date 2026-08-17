package predictor

// Batch processing constants.
const (
	batchSize = 8
)

// EncodeBatch applies horizontal differencing with loop unrolling.
// Optimized for large data arrays.
func EncodeBatch(data []byte) {
	n := len(data)
	if n < 2 {
		return
	}

	// For encoding, we work forward accumulating the running difference
	prev := data[0]
	i := 1

	// Process in batches of 8
	for ; i+batchSize <= n; i += batchSize {
		// Unrolled loop - each iteration depends on the previous
		curr0 := data[i]
		data[i] = curr0 - prev + bias
		prev = curr0

		curr1 := data[i+1]
		data[i+1] = curr1 - prev + bias
		prev = curr1

		curr2 := data[i+2]
		data[i+2] = curr2 - prev + bias
		prev = curr2

		curr3 := data[i+3]
		data[i+3] = curr3 - prev + bias
		prev = curr3

		curr4 := data[i+4]
		data[i+4] = curr4 - prev + bias
		prev = curr4

		curr5 := data[i+5]
		data[i+5] = curr5 - prev + bias
		prev = curr5

		curr6 := data[i+6]
		data[i+6] = curr6 - prev + bias
		prev = curr6

		curr7 := data[i+7]
		data[i+7] = curr7 - prev + bias
		prev = curr7
	}

	// Handle remainder
	for ; i < n; i++ {
		curr := data[i]
		data[i] = curr - prev + bias
		prev = curr
	}
}

// DecodeBatch reverses horizontal differencing with loop unrolling.
// Optimized for large data arrays.
func DecodeBatch(data []byte) {
	n := len(data)
	if n < 2 {
		return
	}

	// For decoding, each element is the sum of itself and all previous
	// This is a prefix sum operation
	i := 1

	// Process in batches of 8 with running sum
	for ; i+batchSize <= n; i += batchSize {
		data[i] = data[i] + data[i-1] - bias
		data[i+1] = data[i+1] + data[i] - bias
		data[i+2] = data[i+2] + data[i+1] - bias
		data[i+3] = data[i+3] + data[i+2] - bias
		data[i+4] = data[i+4] + data[i+3] - bias
		data[i+5] = data[i+5] + data[i+4] - bias
		data[i+6] = data[i+6] + data[i+5] - bias
		data[i+7] = data[i+7] + data[i+6] - bias
	}

	// Handle remainder
	for ; i < n; i++ {
		data[i] = data[i] + data[i-1] - bias
	}
}

// EncodeMultiRow applies horizontal differencing to multiple independent rows.
// Each row is processed independently, which allows for parallel-friendly patterns.
func EncodeMultiRow(data []byte, rowLen, numRows int) {
	if rowLen < 2 || numRows == 0 {
		return
	}

	for row := 0; row < numRows; row++ {
		start := row * rowLen
		end := start + rowLen
		if end > len(data) {
			end = len(data)
		}
		Encode(data[start:end])
	}
}

// DecodeMultiRow reverses horizontal differencing for multiple independent rows.
func DecodeMultiRow(data []byte, rowLen, numRows int) {
	if rowLen < 2 || numRows == 0 {
		return
	}

	for row := 0; row < numRows; row++ {
		start := row * rowLen
		end := start + rowLen
		if end > len(data) {
			end = len(data)
		}
		Decode(data[start:end])
	}
}

// EncodeParallel applies horizontal differencing to independent blocks.
// Blocks are processed independently and can be parallelized.
func EncodeParallel(data []byte, blockSize int, process func(block []byte)) {
	n := len(data)
	if blockSize <= 0 || n == 0 {
		return
	}

	numBlocks := (n + blockSize - 1) / blockSize
	for i := 0; i < numBlocks; i++ {
		start := i * blockSize
		end := start + blockSize
		if end > n {
			end = n
		}
		process(data[start:end])
	}
}
