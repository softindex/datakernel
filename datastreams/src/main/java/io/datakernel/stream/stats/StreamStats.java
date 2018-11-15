package io.datakernel.stream.stats;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialConsumerFunction;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialSupplierFunction;
import io.datakernel.stream.*;

public interface StreamStats<T> extends
		StreamSupplierFunction<T, StreamSupplier<T>>, StreamConsumerFunction<T, StreamConsumer<T>>,
		SerialSupplierFunction<T, SerialSupplier<T>>, SerialConsumerFunction<T, SerialConsumer<T>> {
	StreamDataAcceptor<T> createDataAcceptor(StreamDataAcceptor<T> actualDataAcceptor);

	void onStarted();

	void onProduce();

	void onSuspend();

	void onEndOfStream();

	void onError(Throwable e);

	@Override
	default StreamConsumer<T> apply(StreamConsumer<T> consumer) {
		return consumer.apply(StreamStatsForwarder.create(this));
	}

	@Override
	default StreamSupplier<T> apply(StreamSupplier<T> supplier) {
		return supplier.apply(StreamStatsForwarder.create(this));
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

	static <T> StreamStatsDetailedEx<T> detailedEx(StreamStatsSizeCounter<T> sizeCounter) {
		return new StreamStatsDetailedEx<>(sizeCounter);
	}

}
