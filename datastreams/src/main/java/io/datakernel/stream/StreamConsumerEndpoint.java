package io.datakernel.stream;

import io.datakernel.async.Stage;
import io.datakernel.serial.SerialBuffer;

public final class StreamConsumerEndpoint<T> extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
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
		getSupplier().resume(this);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void accept(T item) {
		assert item != null;
		buffer.add(item);
		if (buffer.isSaturated()) {
			getSupplier().suspend();
		}
	}

	@Override
	protected Stage<Void> onEndOfStream() {
		return buffer.put(null);
	}

	@Override
	protected void onError(Throwable t) {
		buffer.closeWithError(t);
	}

	public Stage<T> take() {
		if (buffer.willBeExhausted()) {
			getSupplier().resume(this);
		}
		return buffer.take();
	}
}
