package streams

import (
	"encoding/binary"
	"hash/fnv"
)

type PartitionedQueue[T any] struct {
	partitions []chan T
}

func channelsNewPartitionedQueue[T any](numPartitions, buffer int) *PartitionedQueue[T] {
	channels := make([]chan T, numPartitions)
	for i := range channels {
		channels[i] = make(chan T, buffer)
	}
	return &PartitionedQueue[T]{partitions: channels}
}

const (
	defaultNumPartitions = 8
	defaultBuffer        = 1024
)

func NewPartitionedQueue[T any]() *PartitionedQueue[T] {
	return channelsNewPartitionedQueue[T](defaultNumPartitions, defaultBuffer)
}

func (queue *PartitionedQueue[T]) PartitionCount() int { return len(queue.partitions) }

func (queue *PartitionedQueue[T]) Publish(partitionKey string, msg T) {
	idx := partitionIndex(partitionKey, len(queue.partitions))
	queue.partitions[idx] <- msg
}

func (queue *PartitionedQueue[T]) Close() {
	for _, ch := range queue.partitions {
		close(ch)
	}
}

func partitionIndex(key string, n int) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	sum := hash.Sum(nil)
	v := binary.LittleEndian.Uint32(sum)
	return int(v % uint32(n))
}
