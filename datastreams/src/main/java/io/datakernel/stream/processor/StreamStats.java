package io.datakernel.stream.processor;

import io.datakernel.stream.DataStreams;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;

import static io.datakernel.stream.DataStreams.stream;

public interface StreamStats {
	<T> StreamDataReceiver<T> createDataReceiver(StreamDataReceiver<T> actualDataReceiver);

	void onStarted();

	void onProduce();

	void onSuspend();

	void onEndOfStream();

	void onError(Throwable throwable);

	default <T> StreamProducer<T> wrap(StreamProducer<T> producer) {
		StreamStatsForwarder<T> statsForwarder = StreamStatsForwarder.create(this);
		DataStreams.stream(producer, statsForwarder.getInput());
		return statsForwarder.getOutput();
	}

	default <T> StreamConsumer<T> wrap(StreamConsumer<T> consumer) {
		StreamStatsForwarder<T> statsForwarder = StreamStatsForwarder.create(this);
		stream(statsForwarder.getOutput(), consumer);
		return statsForwarder.getInput();
	}

}
