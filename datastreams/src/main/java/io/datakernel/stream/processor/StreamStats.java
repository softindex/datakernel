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

	default <T> StreamProducer<T> wrapper(StreamProducer<T> producer) {
		StreamStatsForwarder<T> statsForwarder = StreamStatsForwarder.create(this);
		DataStreams.stream(producer, statsForwarder.getInput());
		return statsForwarder.getOutput();
	}

	default <T> StreamConsumer<T> wrapper(StreamConsumer<T> consumer) {
		StreamStatsForwarder<T> statsForwarder = StreamStatsForwarder.create(this);
		stream(statsForwarder.getOutput(), consumer);
		return statsForwarder.getInput();
	}

	static StreamStatsBasic basic() {
		return new StreamStatsBasic();
	}

	static StreamStatsDetailed detailed() {
		return new StreamStatsDetailed(null);
	}

	static StreamStatsDetailed detailed(StreamStatsSizeCounter<?> sizeCounter) {
		return new StreamStatsDetailed(sizeCounter);
	}

	static StreamStatsDetailedEx detailedEx() {
		return new StreamStatsDetailedEx(null);
	}

	static StreamStatsDetailedEx detailedEx(StreamStatsSizeCounter<?> sizeCounter) {
		return new StreamStatsDetailedEx(sizeCounter);
	}

}
