package io.datakernel.stream;

@FunctionalInterface
public interface StreamProducerModifier<T, R> {
	StreamProducer<R> apply(StreamProducer<T> producer);

	static <T> StreamProducerModifier<T, T> identity() {
		return producer -> producer;
	}
}
