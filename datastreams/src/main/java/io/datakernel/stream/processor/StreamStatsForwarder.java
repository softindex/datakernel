package io.datakernel.stream.processor;

import io.datakernel.stream.*;

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
	}

	private class Output extends AbstractStreamProducer<T> implements StreamDataReceiver<T> {
		private StreamStats.Receiver<T> statsDataReceiver;
		private StreamDataReceiver<T> lastDataReceiver;

		@SuppressWarnings("unchecked")
		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			stats.onProduce();
			if (stats instanceof StreamStats.Receiver) {
				statsDataReceiver = (StreamStats.Receiver<T>) stats;
				lastDataReceiver = dataReceiver;
				input.getProducer().produce(this);
			} else {
				input.getProducer().produce(dataReceiver);
			}
		}

		@Override
		public void onData(T item) {
			statsDataReceiver.onData(item);
			lastDataReceiver.onData(item);
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
	}

}
