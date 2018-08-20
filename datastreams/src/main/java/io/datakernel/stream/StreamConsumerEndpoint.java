package io.datakernel.stream;

import io.datakernel.serial.SerialBuffer;
import io.datakernel.async.Stage;

public final class StreamConsumerEndpoint<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
	public static final int DEFAULT_BUFFER_SIZE = 10;

	private final SerialBuffer<T> buffer;

	public StreamConsumerEndpoint() {
		this(0, DEFAULT_BUFFER_SIZE);
	}

	public StreamConsumerEndpoint(int bufferSize) {
		this(0, bufferSize);
	}

	private StreamConsumerEndpoint(int bufferMinSize, int bufferMaxSize) {
		this.buffer = new SerialBuffer<>(bufferMinSize, bufferMaxSize);
	}

	@Override
	protected void onStarted() {
		getProducer().produce(this);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onData(T item) {
		assert item != null;
		buffer.add(item);
		if (buffer.isSaturated()) {
			getProducer().suspend();
		}
	}

	@Override
	protected void onEndOfStream() {
		buffer.put(null);
	}

	@Override
	protected void onError(Throwable t) {
		buffer.closeWithError(t);
	}

	public Stage<T> take() {
		if (buffer.willBeExhausted()) {
			getProducer().produce(this);
		}
		return buffer.take();
	}

}
