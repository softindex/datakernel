package io.datakernel.stream.stats;

import io.datakernel.stream.*;
import io.datakernel.stream.processor.StreamTransformer;

import java.util.Set;

import static java.util.Collections.emptySet;

public class StreamStatsForwarder<T> implements StreamTransformer<T, T> {
	private final Input input;
	private final Output output;

	private final StreamStats stats;

	private StreamStatsForwarder(StreamStats stats) {
		this.stats = stats;
		this.input = new Input();
		this.output = new Output();
	}

	public static <T> StreamStatsForwarder<T> create(StreamStats stats) {
		return new StreamStatsForwarder<>(stats);
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}

	private class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			stats.onStarted();
		}

		@Override
		protected void onEndOfStream() {
			stats.onEndOfStream();
			output.sendEndOfStream();
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			StreamConsumer<T> consumer = output.getConsumer();
			return consumer != null ? consumer.getCapabilities() : emptySet();
		}
	}

	private class Output extends AbstractStreamProducer<T> {
		@SuppressWarnings("unchecked")
		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			stats.onProduce();
			input.getProducer().produce(stats.createDataReceiver(dataReceiver));
		}

		@Override
		protected void onSuspended() {
			stats.onSuspend();
			input.getProducer().suspend();
		}

		@Override
		protected void onError(Throwable t) {
			stats.onError(t);
			input.closeWithError(t);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			StreamProducer<T> producer = input.getProducer();
			return producer != null ? producer.getCapabilities() : emptySet();
		}
	}

}
