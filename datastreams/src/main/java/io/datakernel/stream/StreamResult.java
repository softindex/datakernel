package io.datakernel.stream;

import io.datakernel.async.Stage;

public interface StreamResult<X, Y> extends StreamProducerResult<X>, StreamConsumerResult<Y>, StreamCompletion {
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

	Stage<Pair<X, Y>> getResult();

	@Override
	default Stage<X> getProducerResult() {
		return getResult().thenApply(Pair::getProducerResult);
	}

	@Override
	default Stage<Y> getConsumerResult() {
		return getResult().thenApply(Pair::getConsumerResult);
	}


}
