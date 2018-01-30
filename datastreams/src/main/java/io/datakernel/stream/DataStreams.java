package io.datakernel.stream;

public class DataStreams {

	public static <T> void bind(StreamProducer<T> producer, StreamConsumer<T> consumer) {
		producer.setConsumer(consumer);
		consumer.setProducer(producer);
	}

	public static <T> StreamingCompletion stream(StreamProducer<T> producer,
	                                             StreamConsumer<T> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, X> StreamingProducerResult<X> stream(StreamProducerWithResult<T, X> producer,
	                                                       StreamConsumer<T> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, Y> StreamingConsumerResult<Y> stream(StreamProducer<T> producer,
	                                                       StreamConsumerWithResult<T, Y> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, X, Y> StreamingResult<X, Y> stream(StreamProducerWithResult<T, X> producer,
	                                                     StreamConsumerWithResult<T, Y> consumer) {
		return producer.streamTo(consumer);
	}

}
