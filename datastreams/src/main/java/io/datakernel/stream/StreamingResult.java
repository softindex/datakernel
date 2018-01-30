package io.datakernel.stream;

import java.util.concurrent.CompletionStage;

public interface StreamingResult<X, Y> extends StreamingProducerResult<X>, StreamingConsumerResult<Y>, StreamingCompletion {
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

	@Override
	default CompletionStage<X> getProducerResult() {
		return getResult().thenApply(Pair::getProducerResult);
	}

	@Override
	default CompletionStage<Y> getConsumerResult() {
		return getResult().thenApply(Pair::getConsumerResult);
	}


}
