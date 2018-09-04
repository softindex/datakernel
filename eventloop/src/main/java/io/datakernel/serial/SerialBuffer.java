package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.functional.Try;

import java.util.ArrayDeque;

import static io.datakernel.util.Recyclable.deepRecycle;

public final class SerialBuffer<T> implements SerialQueue<T>, Cancellable {
	private final ArrayDeque<T> deque = new ArrayDeque<>();
	private final int bufferMinSize;
	private final int bufferMaxSize;

	private SettableStage<Void> put;
	private SettableStage<T> take;

	private boolean endOfStreamReceived;
	private Stage<?> endOfStream;

	public SerialBuffer(int bufferSize) {
		this(0, bufferSize);
	}

	public SerialBuffer(int bufferMinSize, int bufferMaxSize) {
		this.bufferMinSize = bufferMinSize + 1;
		this.bufferMaxSize = bufferMaxSize;
	}

	public boolean isSaturated() {
		return deque.size() > bufferMaxSize;
	}

	public boolean willBeSaturated() {
		return deque.size() >= bufferMaxSize;
	}

	public boolean isExhausted() {
		return deque.size() < bufferMinSize;
	}

	public boolean willBeExhausted() {
		return deque.size() <= bufferMinSize;
	}

	public boolean isPendingPut() {
		return put != null;
	}

	public boolean isPendingTake() {
		return take != null;
	}

	public int size() {
		return deque.size();
	}

	public boolean isEmpty() {
		return deque.isEmpty();
	}

	public boolean isEndOfStream() {
		return endOfStreamReceived && deque.isEmpty();
	}

	@SuppressWarnings("unchecked")
	public void add(@Nullable T value) {
		if (endOfStream == null) {
			if (take != null) {
				assert deque.isEmpty();
				SettableStage<T> take = this.take;
				this.take = null;
				take.set(value);
				return;
			}

			if (value != null) {
				assert !endOfStreamReceived;
				deque.add(value);
				return;
			}

			endOfStreamReceived = true;
			return;
		}

		deepRecycle(value);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Stage<Void> put(@Nullable T value) {
		assert put == null;

		if (endOfStream == null) {
			if (take != null) {
				assert deque.isEmpty();
				SettableStage<T> take = this.take;
				this.take = null;
				take.set(value);
				return Stage.complete();
			}

			if (value != null) {
				assert !endOfStreamReceived;
				deque.add(value);
				if (isSaturated()) {
					put = new SettableStage<>();
					return put;
				} else {
					return Stage.complete();
				}
			}

			endOfStreamReceived = true;
			return Stage.complete();
		}

		deepRecycle(value);
		return (Stage<Void>) endOfStream;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Stage<T> take() {
		assert take == null;

		if (endOfStream != null) return (Stage<T>) endOfStream;

		if (put != null && willBeExhausted()) {
			assert !deque.isEmpty();
			T item = deque.poll();
			SettableStage<Void> put = this.put;
			this.put = null;
			put.set(null);
			return Stage.of(item);
		}

		if (!deque.isEmpty()) {
			return Stage.of(deque.poll());
		}

		if (!endOfStreamReceived) {
			take = new SettableStage<>();
			return take;
		}

		endOfStream = Stage.of(null);
		return (Stage<T>) endOfStream;
	}

	@Nullable
	@SuppressWarnings("unchecked")
	public Try<T> poll() {
		if (endOfStream != null) {
			return (Try<T>) endOfStream.getTry();
		}

		if (put != null && willBeExhausted()) {
			assert !deque.isEmpty();
			T item = deque.poll();
			SettableStage<Void> put = this.put;
			this.put = null;
			put.set(null);
			return Try.of(item);
		}

		if (!deque.isEmpty()) {
			return Try.of(deque.poll());
		}

		if (!endOfStreamReceived) {
			return null;
		}

		endOfStream = Stage.of(null);
		return (Try<T>) endOfStream.getTry();
	}

	@Override
	public void closeWithError(Throwable e) {
		if (put != null) {
			put.setException(e);
			put = null;
		}
		if (take != null) {
			take.setException(e);
			take = null;
		}
		deepRecycle(deque);
		deque.clear();

		endOfStream = Stage.ofException(e);
	}
}
