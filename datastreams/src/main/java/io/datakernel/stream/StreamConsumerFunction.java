package io.datakernel.stream;

@FunctionalInterface
public interface StreamConsumerFunction<T, R> {
	R apply(StreamConsumer<T> consumer);

	static <T> StreamConsumerFunction<T, StreamConsumer<T>> identity() {
		return consumer -> consumer;
	}
}
