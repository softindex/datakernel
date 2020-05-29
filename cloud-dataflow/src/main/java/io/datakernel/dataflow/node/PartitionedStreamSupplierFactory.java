package io.datakernel.dataflow.node;

import io.datakernel.datastream.StreamSupplier;

public interface PartitionedStreamSupplierFactory<T> {
	StreamSupplier<T> get(int partitionIndex, int maxPartitions);
}
