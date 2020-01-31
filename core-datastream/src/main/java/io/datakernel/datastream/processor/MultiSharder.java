package io.datakernel.datastream.processor;

@FunctionalInterface
public interface MultiSharder<K> {

	int[] shard(K key);
}
