package exr

import (
	"sync"
	"sync/atomic"
)

// MemoryLimitExceededError is returned when an allocation would exceed memory limits.
type MemoryLimitExceededError struct {
	Requested int64
	Current   int64
	Limit     int64
}

func (e *MemoryLimitExceededError) Error() string {
	return "exr: memory limit exceeded"
}

// BufferPool manages reusable byte buffers to reduce allocations.
// It supports configurable memory limits to prevent runaway memory usage.
type BufferPool struct {
	pools       []*sync.Pool
	memoryUsed  int64 // atomic: current memory usage in bytes
	memoryLimit int64 // atomic: maximum memory allowed (0 = unlimited)
	allocCount  int64 // atomic: total allocations from pool
	hitCount    int64 // atomic: cache hits
	missCount   int64 // atomic: cache misses
}

// bufferSizes are the discrete sizes for pooled buffers.
// Sizes are chosen to match common EXR chunk sizes.
var bufferSizes = []int{
	1 << 10,   // 1 KB
	4 << 10,   // 4 KB
	16 << 10,  // 16 KB
	64 << 10,  // 64 KB
	256 << 10, // 256 KB
	1 << 20,   // 1 MB
	4 << 20,   // 4 MB
}

// globalBufferPool is the default buffer pool.
var globalBufferPool = NewBufferPool()

// NewBufferPool creates a new buffer pool with no memory limit.
func NewBufferPool() *BufferPool {
	return NewBufferPoolWithLimit(0)
}

// NewBufferPoolWithLimit creates a buffer pool with a memory limit.
// If limit is 0, no limit is enforced.
func NewBufferPoolWithLimit(limit int64) *BufferPool {
	p := &BufferPool{
		pools:       make([]*sync.Pool, len(bufferSizes)),
		memoryLimit: limit,
	}

	for i, size := range bufferSizes {
		size := size // capture for closure
		p.pools[i] = &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, size)
				return &buf
			},
		}
	}

	return p
}

// SetMemoryLimit sets the maximum memory the pool can use.
// If limit is 0, no limit is enforced.
// Returns the previous limit.
func (p *BufferPool) SetMemoryLimit(limit int64) int64 {
	return atomic.SwapInt64(&p.memoryLimit, limit)
}

// MemoryLimit returns the current memory limit (0 = unlimited).
func (p *BufferPool) MemoryLimit() int64 {
	return atomic.LoadInt64(&p.memoryLimit)
}

// MemoryUsed returns the current memory usage in bytes.
func (p *BufferPool) MemoryUsed() int64 {
	return atomic.LoadInt64(&p.memoryUsed)
}

// Stats returns pool statistics: (allocCount, hitCount, missCount).
func (p *BufferPool) Stats() (allocs, hits, misses int64) {
	return atomic.LoadInt64(&p.allocCount),
		atomic.LoadInt64(&p.hitCount),
		atomic.LoadInt64(&p.missCount)
}

// ResetStats resets the pool statistics.
func (p *BufferPool) ResetStats() {
	atomic.StoreInt64(&p.allocCount, 0)
	atomic.StoreInt64(&p.hitCount, 0)
	atomic.StoreInt64(&p.missCount, 0)
}

// poolIndex returns the pool index for a given size.
// Returns -1 if no pool is suitable (size too large).
func poolIndex(size int) int {
	for i, s := range bufferSizes {
		if size <= s {
			return i
		}
	}
	return -1
}

// Get returns a buffer of at least the requested size.
// The returned buffer may be larger than requested.
// Call Put when done to return the buffer to the pool.
// Returns nil if the allocation would exceed the memory limit.
func (p *BufferPool) Get(size int) []byte {
	atomic.AddInt64(&p.allocCount, 1)

	idx := poolIndex(size)
	if idx < 0 {
		// Size too large, allocate directly
		// Check memory limit for large allocations
		limit := atomic.LoadInt64(&p.memoryLimit)
		if limit > 0 {
			current := atomic.LoadInt64(&p.memoryUsed)
			if current+int64(size) > limit {
				return nil
			}
			atomic.AddInt64(&p.memoryUsed, int64(size))
		}
		atomic.AddInt64(&p.missCount, 1)
		return make([]byte, size)
	}

	// Try to get from pool
	pooledSize := bufferSizes[idx]
	limit := atomic.LoadInt64(&p.memoryLimit)
	if limit > 0 {
		current := atomic.LoadInt64(&p.memoryUsed)
		if current+int64(pooledSize) > limit {
			return nil
		}
	}

	bufPtr := p.pools[idx].Get().(*[]byte)
	buf := *bufPtr
	if buf == nil {
		atomic.AddInt64(&p.missCount, 1)
		if limit > 0 {
			atomic.AddInt64(&p.memoryUsed, int64(pooledSize))
		}
		return make([]byte, size)
	}

	atomic.AddInt64(&p.hitCount, 1)
	if limit > 0 {
		atomic.AddInt64(&p.memoryUsed, int64(cap(buf)))
	}

	// Return a slice of the exact size requested
	return buf[:size]
}

// GetWithError returns a buffer or an error if memory limit is exceeded.
func (p *BufferPool) GetWithError(size int) ([]byte, error) {
	buf := p.Get(size)
	if buf == nil {
		limit := atomic.LoadInt64(&p.memoryLimit)
		current := atomic.LoadInt64(&p.memoryUsed)
		return nil, &MemoryLimitExceededError{
			Requested: int64(size),
			Current:   current,
			Limit:     limit,
		}
	}
	return buf, nil
}

// Put returns a buffer to the pool for reuse.
// The buffer must have been obtained from Get.
func (p *BufferPool) Put(buf []byte) {
	if buf == nil {
		return
	}

	// Find the appropriate pool based on capacity
	bufCap := cap(buf)
	idx := poolIndex(bufCap)

	// Update memory tracking
	limit := atomic.LoadInt64(&p.memoryLimit)
	if limit > 0 {
		atomic.AddInt64(&p.memoryUsed, -int64(bufCap))
	}

	if idx < 0 {
		// Buffer too large for any pool, let it be garbage collected
		return
	}

	// Only return to pool if capacity matches the pool size exactly
	if bufCap == bufferSizes[idx] {
		b := buf[:bufCap]
		p.pools[idx].Put(&b)
	}
}

// GetBuffer returns a buffer from the global pool.
func GetBuffer(size int) []byte {
	return globalBufferPool.Get(size)
}

// GetBufferWithError returns a buffer from the global pool or an error.
func GetBufferWithError(size int) ([]byte, error) {
	return globalBufferPool.GetWithError(size)
}

// PutBuffer returns a buffer to the global pool.
func PutBuffer(buf []byte) {
	globalBufferPool.Put(buf)
}

// SetGlobalMemoryLimit sets the memory limit for the global buffer pool.
// If limit is 0, no limit is enforced.
// Returns the previous limit.
func SetGlobalMemoryLimit(limit int64) int64 {
	return globalBufferPool.SetMemoryLimit(limit)
}

// GlobalMemoryLimit returns the current global memory limit.
func GlobalMemoryLimit() int64 {
	return globalBufferPool.MemoryLimit()
}

// GlobalMemoryUsed returns the current memory usage of the global pool.
func GlobalMemoryUsed() int64 {
	return globalBufferPool.MemoryUsed()
}

// GlobalPoolStats returns statistics for the global buffer pool.
func GlobalPoolStats() (allocs, hits, misses int64) {
	return globalBufferPool.Stats()
}

// PooledBuffer wraps a byte slice with automatic pool return.
type PooledBuffer struct {
	Data []byte
	pool *BufferPool
}

// NewPooledBuffer gets a buffer from the pool.
func NewPooledBuffer(size int) *PooledBuffer {
	return &PooledBuffer{
		Data: globalBufferPool.Get(size),
		pool: globalBufferPool,
	}
}

// Release returns the buffer to the pool.
// After Release, the buffer must not be used.
func (b *PooledBuffer) Release() {
	if b.pool != nil && b.Data != nil {
		b.pool.Put(b.Data)
		b.Data = nil
		b.pool = nil
	}
}

// Uint16Pool manages reusable uint16 slices.
type Uint16Pool struct {
	pool sync.Pool
}

// NewUint16Pool creates a pool for uint16 slices.
func NewUint16Pool(defaultSize int) *Uint16Pool {
	return &Uint16Pool{
		pool: sync.Pool{
			New: func() interface{} {
				buf := make([]uint16, defaultSize)
				return &buf
			},
		},
	}
}

// Get returns a uint16 slice of at least the requested size.
func (p *Uint16Pool) Get(size int) []uint16 {
	buf := *p.pool.Get().(*[]uint16)
	if len(buf) < size {
		return make([]uint16, size)
	}
	return buf[:size]
}

// Put returns a uint16 slice to the pool.
func (p *Uint16Pool) Put(buf []uint16) {
	if buf != nil {
		p.pool.Put(&buf)
	}
}
