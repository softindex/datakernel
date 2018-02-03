package io.datakernel.stream.processor;

import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerModifier;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducerModifier;

public interface StreamModifier<I, O> extends StreamProducerModifier<I, O>, StreamConsumerModifier<O, I> {

	static <T> StreamModifier<T, T> identity() {
		return new StreamModifier<T, T>() {
			@Override
			public StreamConsumer<T> applyTo(StreamConsumer<T> consumer) {
				return consumer;
			}

			@Override
			public StreamProducer<T> applyTo(StreamProducer<T> producer) {
				return producer;
			}
		};
	}

}
