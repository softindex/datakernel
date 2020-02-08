package io.datakernel.datastream.processor;

/**
 * A function that calculates multiple shard indices for given objects
 *
 * @see Sharder
 */
@FunctionalInterface
public interface MultiSharder<T> {
	/**
	 * Returns multiple shard indices for given object
	 */
	int[] shard(T key);
}
