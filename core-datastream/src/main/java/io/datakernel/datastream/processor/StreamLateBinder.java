package io.datakernel.datastream.processor;

import io.datakernel.datastream.*;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

import static io.datakernel.common.Utils.nullify;
import static io.datakernel.datastream.StreamCapability.LATE_BINDING;

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

	private int countdown = 2;

	@Nullable
	private StreamDataAcceptor<T> pendingAcceptor;
	@Nullable
	private Throwable pendingException;
	@Nullable
	private SettablePromise<Void> pendingEndOfStreamAck;

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
			if (--countdown == 0) {
				startInputOutput();
			}
		}

		@Override
		protected Promise<Void> onEndOfStream() {
			if (countdown == 0) {
				return output.sendEndOfStream();
			} else {
				pendingEndOfStreamAck = new SettablePromise<>();
				return pendingEndOfStreamAck;
			}
		}

		@Override
		protected void onError(Throwable e) {
			if (countdown == 0) {
				output.close(e);
			} else {
				pendingException = e;
			}
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return extendCapabilities(output.getConsumer(), LATE_BINDING);
		}
	}

	private class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onStarted() {
			if (--countdown == 0) {
				startInputOutput();
			}
		}

		@Override
		protected void onProduce(@NotNull StreamDataAcceptor<T> dataAcceptor) {
			if (countdown == 0) {
				input.getSupplier().resume(dataAcceptor);
			} else {
				pendingAcceptor = dataAcceptor;
			}
		}

		@Override
		protected void onSuspended() {
			if (countdown == 0) {
				input.getSupplier().suspend();
			} else {
				pendingAcceptor = null;
			}
		}

		@Override
		protected void onError(Throwable e) {
			if (countdown == 0) {
				input.close(e);
			} else {
				pendingException = e;
			}
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return extendCapabilities(input.getSupplier(), LATE_BINDING);
		}
	}

	private void startInputOutput() {
		if (pendingException != null) {
			input.close(pendingException);
			output.close(pendingException);
			pendingAcceptor = null;
			pendingEndOfStreamAck = nullify(pendingEndOfStreamAck, SettablePromise::setException, pendingException);
			pendingException = null;
		}
		if (pendingEndOfStreamAck != null) {
			output.sendEndOfStream()
					.whenComplete(pendingEndOfStreamAck);
			pendingAcceptor = null;
		}
		if (pendingAcceptor != null) {
			input.getSupplier().resume(pendingAcceptor);
			pendingAcceptor = null;
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
