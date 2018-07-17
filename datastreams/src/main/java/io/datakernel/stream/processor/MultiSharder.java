package io.datakernel.stream.processor;

@FunctionalInterface
public interface MultiSharder<K> {

	int[] shard(K key);
}
