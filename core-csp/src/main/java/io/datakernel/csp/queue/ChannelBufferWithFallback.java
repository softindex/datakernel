package io.datakernel.csp.queue;

import io.datakernel.async.process.AsyncCloseable;
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

	private boolean finished = false;

	public ChannelBufferWithFallback(ChannelQueue<T> queue, Supplier<Promise<? extends ChannelQueue<T>>> bufferFactory) {
		this.queue = queue;
		this.bufferFactory = bufferFactory;
	}

	private SettablePromise<Void> waitingForBuffer;

	@Override
	public Promise<Void> put(@Nullable T item) {
		if (exception != null) {
			tryRecycle(item);
			return Promise.ofException(exception);
		}
		if (item == null) {
			finished = true;
		}
		if (buffer == null) {
			return primaryPut(item);
		}
		if (!buffer.isSaturated()) {
			return secondaryPut(item);
		}
		buffer.close();
		buffer = null;
		return primaryPut(item);
	}

	@Override
	public Promise<T> take() {
		if (exception != null) {
			return Promise.ofException(exception);
		}
		if (buffer != null) {
			return secondaryTake();
		}
		if (waitingForBuffer != null) {
			return waitingForBuffer.then($ -> secondaryTake());
		}
		return primaryTake();
	}

	private Promise<T> primaryTake() {
		return finished && queue.isExhausted() ?
				Promise.of(null) :
				queue.take();
	}

	private Promise<Void> primaryPut(@Nullable T item) {
		if (!queue.isSaturated()) {
			return queue.put(item);
		}
		SettablePromise<Void> waitingForBuffer = new SettablePromise<>();
		this.waitingForBuffer = waitingForBuffer;
		return bufferFactory.get()
				.then(buffer -> {
					this.buffer = buffer;
					waitingForBuffer.set(null);
					this.waitingForBuffer = null;
					return secondaryPut(item);
				});
	}

	private Promise<T> secondaryTake() {
		assert buffer != null;
		return buffer.take()
				.thenEx((item, e) -> {
					if (e != null) {
						if (e != AsyncCloseable.CLOSE_EXCEPTION) {
							return Promise.ofException(e);
						}
					} else if (item != null) {
						return Promise.of(item);
					} else {
						// here item was null and we had no exception
						buffer.close();
					}
					// here either we had a close excation or item was null
					// so we retry the whole thing (same as in secondaryPut)
					buffer = null;
					return primaryTake();
				});
	}

	private Promise<Void> secondaryPut(@Nullable T item) {
		assert buffer != null;
		return buffer.put(item)
				.thenEx(($, e) -> {
					if (e == null) {
						return Promise.complete();
					}
					if (e != AsyncCloseable.CLOSE_EXCEPTION) {
						return Promise.ofException(e);
					}
					// buffer was already closed for whatever reason,
					// retry the whole thing (may cause loops, but should not)
					buffer = null;
					return primaryPut(item);
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
