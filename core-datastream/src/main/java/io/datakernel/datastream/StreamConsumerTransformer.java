package io.datakernel.datastream;

@FunctionalInterface
public interface StreamConsumerTransformer<T, R> {
	R transform(StreamConsumer<T> consumer);

	static <T> StreamConsumerTransformer<T, StreamConsumer<T>> identity() {
		return consumer -> consumer;
	}
}
