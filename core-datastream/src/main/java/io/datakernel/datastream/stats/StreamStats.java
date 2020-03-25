package io.datakernel.datastream.stats;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.dsl.ChannelConsumerTransformer;
import io.datakernel.csp.dsl.ChannelSupplierTransformer;
import io.datakernel.datastream.*;

public interface StreamStats<T> extends
		StreamSupplierTransformer<T, StreamSupplier<T>>, StreamConsumerTransformer<T, StreamConsumer<T>>,
		ChannelSupplierTransformer<T, ChannelSupplier<T>>, ChannelConsumerTransformer<T, ChannelConsumer<T>> {
	StreamDataAcceptor<T> createDataAcceptor(StreamDataAcceptor<T> actualDataAcceptor);

	void onStarted();

	void onResume();

	void onSuspend();

	void onEndOfStream();

	void onError(Throwable e);

	@Override
	default StreamConsumer<T> transform(StreamConsumer<T> consumer) {
		return consumer.transformWith(StreamStatsForwarder.create(this));
	}

	@Override
	default StreamSupplier<T> transform(StreamSupplier<T> supplier) {
		return supplier.transformWith(StreamStatsForwarder.create(this));
	}

	@Override
	default ChannelSupplier<T> transform(ChannelSupplier<T> supplier) {
		return supplier; // TODO
	}

	@Override
	default ChannelConsumer<T> transform(ChannelConsumer<T> consumer) {
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
