package io.datakernel.stream;

import java.util.List;

public interface StreamProducerModifier<T, R> {
	StreamProducer<R> applyTo(StreamProducer<T> producer);

	default <X> StreamProducerWithResult<R, X> applyTo(StreamProducerWithResult<T, X> producer) {
		return applyTo((StreamProducer<T>) producer).withResult(producer.getResult());
	}

	default <X> StreamProducerModifier<T, X> then(StreamProducerModifier<R, X> nextModifier) {
		return producer -> nextModifier.applyTo(this.applyTo(producer));
	}

	static <T> StreamProducerModifier<T, T> identity() {
		return producer -> producer;
	}

	static <T> StreamProducerModifier<T, T> of(List<StreamProducerModifier<T, T>> modifiers) {
		return producer -> {
			for (StreamProducerModifier<T, T> modifier : modifiers) {
				producer = modifier.applyTo(producer);
			}
			return producer;
		};
	}

}
