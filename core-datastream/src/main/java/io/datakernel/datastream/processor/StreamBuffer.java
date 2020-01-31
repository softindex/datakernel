package io.datakernel.datastream.processor;

import io.datakernel.datastream.*;
import io.datakernel.promise.Promise;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

import static io.datakernel.datastream.StreamCapability.IMMEDIATE_SUSPEND;

public class StreamBuffer<T> implements StreamTransformer<T, T> {

	private final Input input;
	private final Output output;

	private final Deque<T> buffer = new ArrayDeque<>();
	private final int minBuffered;
	private final int maxBuffered;

	private boolean suspended = false; // TODO

	// region creators
	private StreamBuffer(int minBuffered, int maxBuffered) {
		this.minBuffered = minBuffered;
		this.maxBuffered = maxBuffered;
		this.input = new Input();
		this.output = new Output();
	}

	public static <T> StreamBuffer<T> create() {
		return new StreamBuffer<>(0, 0);
	}

	public static <T> StreamBuffer<T> create(int minBuffered, int maxBuffered) {
		return new StreamBuffer<>(minBuffered, maxBuffered);
	}
	// endregion

	protected final class Input extends AbstractStreamConsumer<T> {
		@Override
		public Set<StreamCapability> getCapabilities() {
			return extendCapabilities(output.getConsumer(), IMMEDIATE_SUSPEND);
		}

		@Override
		protected Promise<Void> onEndOfStream() {
			output.tryProduce();
			return output.getConsumer().getAcknowledgement();
		}

		@Override
		protected void onError(Throwable e) {
			output.close(e);
		}
	}

	protected final class Output extends AbstractStreamSupplier<T> implements StreamDataAcceptor<T> {
		@Override
		public void accept(T item) {
			if (suspended) {
				buffer.offer(item);
				if (buffer.size() >= maxBuffered) {
					input.getSupplier().suspend();
				}
				return;
			}
			output.getLastDataAcceptor().accept(item);
		}

		@Override
		protected void produce(AsyncProduceController async) {
			while (!buffer.isEmpty()) {
				if (!output.isReceiverReady()) {
					return;
				}
				send(buffer.pop());
				if (buffer.size() < minBuffered) {
					input.getSupplier().resume(this);
				}
			}
			if (output.isReceiverReady()) {
				suspended = false;
				input.getSupplier().resume(this);
			}
			if (input.getEndOfStream().isResult()) {
				sendEndOfStream();
			}
		}

		@Override
		protected void onSuspended() {
			suspended = true;
			if (maxBuffered == 0) {
				input.getSupplier().suspend();
			}
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return extendCapabilities(input.getSupplier(), IMMEDIATE_SUSPEND);
		}

		@Override
		protected void onError(Throwable e) {
			input.close(e);
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
