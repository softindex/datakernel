package io.datakernel.stream;

import io.datakernel.async.Promise;
import io.datakernel.csp.queue.ChannelBuffer;

public final class StreamConsumerEndpoint<T> extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
	public static final int DEFAULT_BUFFER_SIZE = 10;

	private final ChannelBuffer<T> buffer;

	public StreamConsumerEndpoint() {
		this(0, DEFAULT_BUFFER_SIZE);
	}

	public StreamConsumerEndpoint(int bufferSize) {
		this(0, bufferSize);
	}

	private StreamConsumerEndpoint(int bufferMinSize, int bufferMaxSize) {
		this.buffer = new ChannelBuffer<>(bufferMinSize, bufferMaxSize);
	}

	@Override
	protected void onStarted() {
		getSupplier().resume(this);
	}

	@Override
	public void accept(T item) {
		assert item != null;
		try {
			buffer.add(item);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			close(e);
			return;
		}
		if (buffer.isSaturated()) {
			getSupplier().suspend();
		}
	}

	@Override
	protected Promise<Void> onEndOfStream() {
		return buffer.put(null);
	}

	@Override
	protected void onError(Throwable e) {
		buffer.close(e);
	}

	public Promise<T> take() {
		if (buffer.willBeExhausted()) {
			getSupplier().resume(this);
		}
		return buffer.take();
	}
}
