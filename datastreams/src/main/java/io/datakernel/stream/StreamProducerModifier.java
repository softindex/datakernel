package io.datakernel.stream;

import java.util.List;

public interface StreamProducerModifier<I, O> {
	StreamProducer<O> apply(StreamProducer<I> producer);

	default <T> StreamProducerModifier<I, T> then(StreamProducerModifier<O, T> nextModifier) {
		return producer -> nextModifier.apply(this.apply(producer));
	}

	static <T> StreamProducerModifier<T, T> identity() {
		return producer -> producer;
	}

	static <T> StreamProducerModifier<T, T> of(List<StreamProducerModifier<T, T>> modifiers) {
		return producer -> {
			for (StreamProducerModifier<T, T> modifier : modifiers) {
				producer = modifier.apply(producer);
			}
			return producer;
		};
	}

}
