package io.datakernel.stream.stats;

import io.datakernel.stream.*;

public interface StreamStats<T> extends StreamConsumerModifier<T, T>, StreamProducerModifier<T, T> {
	StreamDataReceiver<T> createDataReceiver(StreamDataReceiver<T> actualDataReceiver);

	void onStarted();

	void onProduce();

	void onSuspend();

	void onEndOfStream();

	void onError(Throwable throwable);

	@Override
	default StreamConsumer<T> apply(StreamConsumer<T> consumer) {
		return consumer.with(StreamStatsForwarder.create(this));
	}

	@Override
	default StreamProducer<T> apply(StreamProducer<T> producer) {
		return producer.with(StreamStatsForwarder.create(this));
	}

	static <T> StreamStatsBasic<T> basic() {
		return new StreamStatsBasic<>();
	}

	static <T> StreamStatsDetailed<T> detailed() {
		return new StreamStatsDetailed<>(null);
	}

	static <T> StreamStatsDetailed<T> detailed(StreamStatsSizeCounter<T> sizeCounter) {
		return new StreamStatsDetailed<>(sizeCounter);
	}

	static <T> StreamStatsDetailedEx<T> detailedEx() {
		return new StreamStatsDetailedEx<>(null);
	}

	static <T> StreamStatsDetailedEx detailedEx(StreamStatsSizeCounter<T> sizeCounter) {
		return new StreamStatsDetailedEx<>(sizeCounter);
	}

}
