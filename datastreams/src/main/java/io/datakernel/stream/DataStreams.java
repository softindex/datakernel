package io.datakernel.stream;

import java.util.concurrent.CompletionStage;

public class DataStreams {

	public static <T> void bind(StreamProducer<T> producer, StreamConsumer<T> consumer) {
		producer.setConsumer(consumer);
		consumer.setProducer(producer);
	}

	public interface StreamResult {
		CompletionStage<Void> getProducerEndOfStream();

		CompletionStage<Void> getConsumerEndOfStream();

		CompletionStage<Void> getEndOfStream();
	}

	public interface ProducerResult<X> extends StreamResult {
		CompletionStage<X> getProducerResult();
	}

	public interface ConsumerResult<Y> extends StreamResult {
		CompletionStage<Y> getConsumerResult();
	}

	public interface ProducerConsumerResult<X, Y> extends ProducerResult<X>, ConsumerResult<Y> {
		final class Pair<X, Y> {
			private final X producerResult;
			private final Y consumerResult;

			public Pair(X producerResult, Y consumerResult) {
				this.producerResult = producerResult;
				this.consumerResult = consumerResult;
			}

			public X getProducerResult() {
				return producerResult;
			}

			public Y getConsumerResult() {
				return consumerResult;
			}
		}

		CompletionStage<Pair<X, Y>> getResult();
	}

	public static <T> StreamResult stream(StreamProducer<T> producer,
	                                      StreamConsumer<T> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, X> ProducerResult<X> stream(StreamProducerWithResult<T, X> producer,
	                                              StreamConsumer<T> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, Y> ConsumerResult<Y> stream(StreamProducer<T> producer,
	                                              StreamConsumerWithResult<T, Y> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, X, Y> ProducerConsumerResult<X, Y> stream(StreamProducerWithResult<T, X> producer,
	                                                            StreamConsumerWithResult<T, Y> consumer) {
		return producer.streamTo(consumer);
	}

}
