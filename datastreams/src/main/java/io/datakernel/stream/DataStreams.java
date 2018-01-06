package io.datakernel.stream;

import io.datakernel.async.Stages;

import java.util.concurrent.CompletionStage;

public class DataStreams {

	public static <T> CompletionStage<Void> stream(StreamProducer<T> producer,
	                                               StreamConsumer<T> consumer) {
		bind(producer, consumer);
		return Stages.pair(producer.getEndOfStream(), consumer.getEndOfStream())
				.thenApply($ -> null);
	}

	public static <T, X> CompletionStage<X> stream(StreamProducerWithResult<T, X> producer,
	                                               StreamConsumer<T> consumer) {
		bind(producer, consumer);
		return Stages.pair(producer.getResult(), consumer.getEndOfStream())
				.thenApply(Stages.Pair::getLeft);
	}

	public static <T, Y> CompletionStage<Y> stream(StreamProducer<T> producer,
	                                               StreamConsumerWithResult<T, Y> consumer) {
		bind(producer, consumer);
		return Stages.pair(producer.getEndOfStream(), consumer.getResult())
				.thenApply(Stages.Pair::getRight);
	}

	public static final class ProducerConsumerResult<X, Y> {

		private final X producerResult;
		private final Y consumerResult;

		private ProducerConsumerResult(X producerResult, Y consumerResult) {
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

	public static <T, X, Y> CompletionStage<ProducerConsumerResult<X, Y>> stream(StreamProducerWithResult<T, X> producer,
	                                                                             StreamConsumerWithResult<T, Y> consumer) {
		bind(producer, consumer);
		return Stages.pair(producer.getResult(), consumer.getResult())
				.thenApply(pair -> new ProducerConsumerResult<>(pair.getLeft(), pair.getRight()));
	}

	public static <T> void bind(StreamProducer<T> producer, StreamConsumer<T> consumer) {
		producer.setConsumer(consumer);
		consumer.setProducer(producer);
	}
}
