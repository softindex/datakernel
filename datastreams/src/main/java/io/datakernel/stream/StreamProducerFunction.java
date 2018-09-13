package io.datakernel.stream;

@FunctionalInterface
public interface StreamProducerFunction<T, R> {
	R apply(StreamProducer<T> producer);

	static <T> StreamProducerFunction<T, StreamProducer<T>> identity() {
		return producer -> producer;
	}
}
