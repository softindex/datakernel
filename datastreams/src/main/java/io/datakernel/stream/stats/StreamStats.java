package io.datakernel.stream.stats;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialConsumerFunction;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialSupplierFunction;
import io.datakernel.stream.*;

public interface StreamStats<T> extends
		StreamProducerFunction<T, StreamProducer<T>>, StreamConsumerFunction<T, StreamConsumer<T>>,
		SerialSupplierFunction<T, SerialSupplier<T>>, SerialConsumerFunction<T, SerialConsumer<T>> {
	StreamDataAcceptor<T> createDataAcceptor(StreamDataAcceptor<T> actualDataAcceptor);

	void onStarted();

	void onProduce();

	void onSuspend();

	void onEndOfStream();

	void onError(Throwable throwable);

	@Override
	default StreamConsumer<T> apply(StreamConsumer<T> consumer) {
		return consumer.apply(StreamStatsForwarder.create(this));
	}

	@Override
	default StreamProducer<T> apply(StreamProducer<T> producer) {
		return producer.apply(StreamStatsForwarder.create(this));
	}

	@Override
	default SerialSupplier<T> apply(SerialSupplier<T> supplier) {
		return supplier; // TODO
	}

	@Override
	default SerialConsumer<T> apply(SerialConsumer<T> consumer) {
		return consumer; // TODO
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
