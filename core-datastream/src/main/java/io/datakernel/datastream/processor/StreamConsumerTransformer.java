package io.datakernel.datastream.processor;

import io.datakernel.datastream.StreamConsumer;

/**
 * A transformer function that converts {@link StreamConsumer suppliers} into something else.
 * Part of the {@link StreamConsumer#transformWith} DSL.
 */
@FunctionalInterface
public interface StreamConsumerTransformer<T, R> {
	R transform(StreamConsumer<T> consumer);

	static <T> StreamConsumerTransformer<T, StreamConsumer<T>> identity() {
		return consumer -> consumer;
	}
}
