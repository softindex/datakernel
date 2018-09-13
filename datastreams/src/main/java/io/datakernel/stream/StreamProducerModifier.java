package io.datakernel.stream;

@FunctionalInterface
public interface StreamProducerModifier<T, R> {
	R apply(StreamProducer<T> producer);

	static <T> StreamProducerModifier<T, StreamProducer<T>> identity() {
		return producer -> producer;
	}
}
