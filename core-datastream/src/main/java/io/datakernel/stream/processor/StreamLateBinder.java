package io.datakernel.stream.processor;

import io.datakernel.async.Promise;
import io.datakernel.stream.*;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;

/**
 * If stream consumer is not immediately wired, on next eventloop cycle it will error out.
 * This is because consumers request suppliers to start producing items on the next cycle after they're wired.
 * <p>
 * This transformer solves that by storing a data receiver from consumer produce request if it is not wired
 * and when it is actually wired request his new supplier to produce into that stored receiver.
 */
public final class StreamLateBinder<T> implements StreamTransformer<T, T> {
	private final AbstractStreamConsumer<T> input = new Input();
	private final AbstractStreamSupplier<T> output = new Output();

	@Nullable
	private StreamDataAcceptor<T> waitingAcceptor;

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
			if (waitingAcceptor != null) {
				getSupplier().resume(waitingAcceptor);
				waitingAcceptor = null;
			}
		}

		@Override
		protected Promise<Void> onEndOfStream() {
			return output.sendEndOfStream();
		}

		@Override
		protected void onError(Throwable e) {
			output.close(e);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return addCapabilities(output.getConsumer(), LATE_BINDING);
		}
	}

	private class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onProduce(StreamDataAcceptor<T> dataAcceptor) {
			StreamSupplier<T> supplier = input.getSupplier();
			if (supplier == null) {
				waitingAcceptor = dataAcceptor;
				return;
			}
			supplier.resume(dataAcceptor);
		}

		@Override
		protected void onSuspended() {
			StreamSupplier<T> supplier = input.getSupplier();
			if (supplier == null) {
				waitingAcceptor = null;
				return;
			}
			supplier.suspend();
		}

		@Override
		protected void onError(Throwable e) {
			input.close(e);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return addCapabilities(input.getSupplier(), LATE_BINDING);
		}
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}
}
