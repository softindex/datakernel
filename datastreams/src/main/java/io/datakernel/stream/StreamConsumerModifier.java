package io.datakernel.stream;

import java.util.List;

public interface StreamConsumerModifier<T, R> {
	StreamConsumer<R> applyTo(StreamConsumer<T> consumer);

	default <X> StreamConsumerWithResult<R, X> applyTo(StreamConsumerWithResult<T, X> consumer) {
		return applyTo((StreamConsumer<T>) consumer).withResult(consumer.getResult());
	}

	default <X> StreamConsumerModifier<T, X> then(StreamConsumerModifier<R, X> nextModifier) {
		return consumer -> nextModifier.applyTo(this.applyTo(consumer));
	}

	static <T> StreamConsumerModifier<T, T> identity() {
		return consumer -> consumer;
	}

	static <T> StreamConsumerModifier<T, T> of(List<StreamConsumerModifier<T, T>> modifiers) {
		return consumer -> {
			for (StreamConsumerModifier<T, T> modifier : modifiers) {
				consumer = modifier.applyTo(consumer);
			}
			return consumer;
		};
	}
}
