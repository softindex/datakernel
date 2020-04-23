package io.datakernel.csp.queue;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

import static io.datakernel.common.Recyclable.tryRecycle;

public final class ChannelBufferWithFallback<T> implements ChannelQueue<T> {
	private final ChannelQueue<T> queue;
	private final Supplier<Promise<? extends ChannelQueue<T>>> bufferFactory;

	@Nullable
	private ChannelQueue<T> buffer;

	@Nullable
	private Exception exception;

	public ChannelBufferWithFallback(ChannelQueue<T> queue, Supplier<Promise<? extends ChannelQueue<T>>> bufferFactory) {
		this.queue = queue;
		this.bufferFactory = bufferFactory;
	}

	private SettablePromise<ByteBuf> bufferTake;

	@Override
	public Promise<Void> put(@Nullable T item) {
		if (exception != null) {
			tryRecycle(item);
			return Promise.ofException(exception);
		}
		if (buffer != null) {
			if (buffer.isExhausted()) {
				buffer.close();
				buffer = null;
			} else {
				return buffer.put(item);
			}
		}
		if (!queue.isSaturated()) {
			return queue.put(item);
		}
		SettablePromise<ByteBuf> promise = new SettablePromise<>();
		bufferTake = promise;
		return bufferFactory.get()
				.then(buffer -> {
					this.buffer = buffer;
					promise.set(null);
					bufferTake = null;
					return buffer.put(item);
				});
	}

	@Override
	public Promise<T> take() {
		if (exception != null) {
			return Promise.ofException(exception);
		}
		if (buffer == null) {
			if (bufferTake != null) {
				return bufferTake.then($ -> buffer.take());
			}
			return queue.take();
		}
		if (buffer.isExhausted()) {
			buffer.close();
			buffer = null;
			return queue.take();
		}
		return buffer.take()
				.then(item -> {
					if (item != null) {
						return Promise.of(item);
					}
					buffer.close();
					buffer = null;
					return queue.take();
				});
	}

	@Override
	public boolean isSaturated() {
		return queue.isSaturated() && buffer != null && buffer.isSaturated();
	}

	@Override
	public boolean isExhausted() {
		return queue.isExhausted() && (buffer == null || buffer.isExhausted());
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
		if (exception != null) return;
		exception = e instanceof Exception ? (Exception) e : new RuntimeException(e);
		queue.closeEx(e);
		if (buffer != null) {
			buffer.closeEx(e);
		}
	}

	@Nullable
	public Throwable getException() {
		return exception;
	}
}
