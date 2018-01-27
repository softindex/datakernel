package io.datakernel.stream;

import java.util.List;

public interface StreamConsumerModifier<I, O> {
	StreamConsumer<O> apply(StreamConsumer<I> consumer);

	default <T> StreamConsumerModifier<I, T> then(StreamConsumerModifier<O, T> nextModifier) {
		return consumer -> nextModifier.apply(this.apply(consumer));
	}

	static <T> StreamConsumerModifier<T, T> identity() {
		return consumer -> consumer;
	}

	static <T> StreamConsumerModifier<T, T> of(List<StreamConsumerModifier<T, T>> modifiers) {
		return consumer -> {
			for (StreamConsumerModifier<T, T> modifier : modifiers) {
				consumer = modifier.apply(consumer);
			}
			return consumer;
		};
	}
}
