package io.datakernel.stream;

@FunctionalInterface
public interface StreamConsumerModifier<T, R> {
	R apply(StreamConsumer<T> consumer);

	static <T> StreamConsumerModifier<T, StreamConsumer<T>> identity() {
		return consumer -> consumer;
	}
}
