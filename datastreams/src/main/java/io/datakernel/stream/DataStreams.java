package io.datakernel.stream;

public class DataStreams {

	public static <T> void bind(StreamProducer<T> producer, StreamConsumer<T> consumer) {
		producer.setConsumer(consumer);
		consumer.setProducer(producer);
	}

	public static <T> StreamCompletion stream(StreamProducer<T> producer,
											  StreamConsumer<T> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, X> StreamProducerResult<X> stream(StreamProducerWithResult<T, X> producer,
														StreamConsumer<T> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, Y> StreamConsumerResult<Y> stream(StreamProducer<T> producer,
														StreamConsumerWithResult<T, Y> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, X, Y> StreamResult<X, Y> stream(StreamProducerWithResult<T, X> producer,
													  StreamConsumerWithResult<T, Y> consumer) {
		return producer.streamTo(consumer);
	}

}
