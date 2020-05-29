package io.datakernel.dataflow.node;

import io.datakernel.datastream.StreamConsumer;

public interface PartitionedStreamConsumerFactory<T> {
	StreamConsumer<T> get(int partitionIndex, int maxPartitions);
}
