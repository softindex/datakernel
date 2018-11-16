package io.datakernel.stream;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.csp.queue.ChannelBuffer;

public final class StreamSupplierEndpoint<T> extends AbstractStreamSupplier<T> {
	public static final int DEFAULT_BUFFER_SIZE = 10;

	private final ChannelBuffer<T> buffer;

	public StreamSupplierEndpoint() {
		this(0, DEFAULT_BUFFER_SIZE);
	}

	public StreamSupplierEndpoint(int bufferSize) {
		this(0, bufferSize);
	}

	private StreamSupplierEndpoint(int bufferMinSize, int bufferMaxSize) {
		this.buffer = new ChannelBuffer<>(bufferMinSize, bufferMaxSize);
	}

	public void add(T item) throws Exception {
		postProduce();
		buffer.add(item);
	}

	public Promise<Void> put(@Nullable T item) {
		postProduce();
		return buffer.put(item);
	}

	@Override
	protected void produce(AsyncProduceController async) {
		try {
			while (!buffer.isEmpty()) {
				T item = buffer.poll();
				if (item != null) {
					send(item);
				} else {
					sendEndOfStream();
				}
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			close(e);
		}
	}

	@Override
	protected void onError(Throwable e) {
		buffer.close(e);
	}
}
