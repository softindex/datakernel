package io.datakernel.stream.processor;

import io.datakernel.annotation.Nullable;
import io.datakernel.stream.*;

import java.util.Set;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;

/**
 * If stream consumer is not immediately wired, on next eventloop cycle it will error out.
 * This is because consumers request producers to start producing items on the next cycle after they're wired.
 * <p>
 * This transformer solves that by storing a data receiver from consumer produce request if it is not wired
 * and when it is actually wired request his new producer to produce into that stored receiver.
 */
public final class StreamLateBinder<T> implements StreamTransformer<T, T> {
	private final AbstractStreamConsumer<T> input = new Input();
	private final AbstractStreamProducer<T> output = new Output();

	@Nullable
	private StreamDataReceiver<T> waitingReceiver;

	// region creators
	private StreamLateBinder() {
	}

	public static <T> StreamLateBinder<T> create() {
		return new StreamLateBinder<>();
	}
	// endregion

	private class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			if (waitingReceiver != null) {
				getProducer().produce(waitingReceiver);
				waitingReceiver = null;
			}
		}

		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return addCapabilities(output.getConsumer(), LATE_BINDING);
		}
	}

	private class Output extends AbstractStreamProducer<T> {
		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			StreamProducer<T> producer = input.getProducer();
			if (producer == null) {
				waitingReceiver = dataReceiver;
				return;
			}
			producer.produce(dataReceiver);
		}

		@Override
		protected void onSuspended() {
			StreamProducer<T> producer = input.getProducer();
			if (producer == null) {
				waitingReceiver = null;
				return;
			}
			producer.suspend();
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return addCapabilities(input.getProducer(), LATE_BINDING);
		}
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}
}