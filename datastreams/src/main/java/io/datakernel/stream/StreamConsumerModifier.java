package io.datakernel.stream;

@FunctionalInterface
public interface StreamConsumerModifier<T, R> {
	StreamConsumer<R> apply(StreamConsumer<T> consumer);

	static <T> StreamConsumerModifier<T, T> identity() {
		return consumer -> consumer;
	}
}
