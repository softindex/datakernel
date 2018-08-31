package io.datakernel.async;

import io.datakernel.annotation.Nullable;
import io.datakernel.functional.Try;

import java.util.ArrayDeque;

public final class AsyncBuffer<T> {
	private final ArrayDeque<T> deque = new ArrayDeque<>();
	private final int bufferMinSize;
	private final int bufferMaxSize;

	private SettableStage<Void> put;
	private SettableStage<T> take;

	public AsyncBuffer(int bufferSize) {
		this(0, bufferSize);
	}

	public AsyncBuffer(int bufferMinSize, int bufferMaxSize) {
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

	@SuppressWarnings("unchecked")
	public void add(T value) {
		assert value != null;
		if (take != null) {
			assert deque.isEmpty();
			SettableStage<T> take = this.take;
			this.take = null;
			take.set(value);
			return;
		}

		deque.add(value);
	}

	@SuppressWarnings("unchecked")
	public Stage<Void> put(T value) {
		assert value != null;
		assert put == null;

		if (take != null) {
			assert deque.isEmpty();
			SettableStage<T> take = this.take;
			this.take = null;
			take.set(value);
			return Stage.complete();
		}

		deque.add(value);
		if (isSaturated()) {
			put = new SettableStage<>();
			return put;
		} else {
			return Stage.complete();
		}
	}

	@SuppressWarnings("unchecked")
	public Stage<T> take() {
		assert take == null;

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

		take = new SettableStage<>();
		return take;
	}

	@Nullable
	public Try<T> poll() {
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

		return null;
	}

}
