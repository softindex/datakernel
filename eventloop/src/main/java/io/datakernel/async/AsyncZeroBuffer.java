package io.datakernel.async;

import io.datakernel.annotation.Nullable;

public final class AsyncZeroBuffer<T> {
	@Nullable
	private T value;

	private SettableStage<Void> put;
	private SettableStage<T> take;

	public boolean isSaturated() {
		return !isEmpty(); // size() > 0;
	}

	public boolean willBeSaturated() {
		return true; // size() >= 0;
	}

	public boolean isExhausted() {
		return isEmpty(); // size() < 1;
	}

	public boolean willBeExhausted() {
		return true; // size() <= 1;
	}

	public boolean isPendingPut() {
		return put != null;
	}

	public boolean isPendingTake() {
		return take != null;
	}

	public int size() {
		return value != null ? 1 : 0;
	}

	public boolean isEmpty() {
		return value == null;
	}

	private void add(T value) {
		assert value != null;
		assert isEmpty();
		this.value = value;
	}

	private T poll() {
		assert !isEmpty();
		T value = this.value;
		this.value = null;
		return value;
	}

	@SuppressWarnings("unchecked")
	public Stage<Void> put(T value) {
		assert value != null;
		assert put == null;

		if (take != null) {
			assert isEmpty();
			SettableStage<T> take = this.take;
			this.take = null;
			take.set(value);
			return Stage.complete();
		}

		add(value);
		put = new SettableStage<>();
		return put;
	}

	@SuppressWarnings("unchecked")
	public Stage<T> take() {
		assert take == null;

		if (put != null) {
			T item = poll();
			SettableStage<Void> put = this.put;
			this.put = null;
			put.set(null);
			return Stage.of(item);
		}

		if (!isEmpty()) {
			return Stage.of(poll());
		}

		take = new SettableStage<>();
		return take;
	}

}
