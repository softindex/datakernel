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

		this.value = value;
		put = new SettableStage<>();
		return put;
	}

	@SuppressWarnings("unchecked")
	public Stage<T> take() {
		assert take == null;

		if (put != null) {
			T value = this.value;
			SettableStage<Void> put = this.put;
			this.value = null;
			this.put = null;
			put.set(null);
			return Stage.of(value);
		}

		if (!isEmpty()) {
			T value = this.value;
			this.value = null;
			return Stage.of(value);
		}

		take = new SettableStage<>();
		return take;
	}

}
