package io.datakernel.stream.processor;

import io.datakernel.stream.*;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

import static io.datakernel.stream.StreamCapability.IMMEDIATE_SUSPEND;

public class StreamSuspendBuffer<T> implements StreamTransformer<T, T>, StreamDataReceiver<T> {

	private final Input input;
	private final Output output;

	private final Deque<T> buffer = new ArrayDeque<>();
	private final int minBuffered;
	private final int maxBuffered;

	private boolean suspended = false;
	private boolean finished = false;

	// region creators
	private StreamSuspendBuffer(int minBuffered, int maxBuffered) {
		this.minBuffered = minBuffered;
		this.maxBuffered = maxBuffered;
		this.input = new Input();
		this.output = new Output();
	}

	public static <T> StreamSuspendBuffer<T> create() {
		return new StreamSuspendBuffer<>(0, 0);
	}

	public static <T> StreamSuspendBuffer<T> create(int minBuffered, int maxBuffered) {
		return new StreamSuspendBuffer<>(minBuffered, maxBuffered);
	}
	// endregion

	@Override
	public void onData(T item) {
		if (suspended) {
			buffer.offer(item);
			if (buffer.size() >= maxBuffered) {
				input.getProducer().suspend();
			}
			return;
		}
		output.send(item);
	}

	protected final class Input extends AbstractStreamConsumer<T> {
		@Override
		public Set<StreamCapability> getCapabilities() {
			return addCapabilities(output.getConsumer(), IMMEDIATE_SUSPEND);
		}

		@Override
		protected void onEndOfStream() {
			if (!suspended) {
				output.sendEndOfStream();
				return;
			}
			finished = true;
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
		}
	}

	protected final class Output extends AbstractStreamProducer<T> {
		@Override
		protected void produce(AsyncProduceController async) {
			while (!buffer.isEmpty()) {
				if (!output.isReceiverReady()) {
					return;
				}
				send(buffer.pop());
				if (buffer.size() < minBuffered) {
					input.getProducer().produce(StreamSuspendBuffer.this);
				}
			}
			if (output.isReceiverReady()) {
				suspended = false;
				input.getProducer().produce(StreamSuspendBuffer.this);
			}
			if (finished) {
				sendEndOfStream();
			}
		}

		@Override
		protected void onSuspended() {
			suspended = true;
			if (maxBuffered == 0) {
				input.getProducer().suspend();
			}
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return addCapabilities(input.getProducer(), IMMEDIATE_SUSPEND);
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
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