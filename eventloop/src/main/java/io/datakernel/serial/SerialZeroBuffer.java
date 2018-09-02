package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.CompleteStage;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;

import static io.datakernel.util.Recyclable.deepRecycle;

public final class SerialZeroBuffer<T> implements SerialQueue<T>, Cancellable {
	@Nullable
	private CompleteStage<?> endOfStream;
	@Nullable
	private T value;

	private SettableStage<Void> put;
	private SettableStage<T> take;

	private boolean endOfStreamReceived;

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

	public boolean isEndOfStream() {
		return endOfStreamReceived && isEmpty();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Stage<Void> put(@Nullable T value) {
		assert put == null;

		if (endOfStream == null) {
			if (take != null) {
				assert isEmpty();
				SettableStage<T> take = this.take;
				this.take = null;
				take.set(value);
				return Stage.complete();
			}

			if (value != null) {
				assert !endOfStreamReceived;
				add(value);
				put = new SettableStage<>();
				return put;
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

		if (!endOfStreamReceived) {
			take = new SettableStage<>();
			return take;
		}

		endOfStream = CompleteStage.of(null);
		return (Stage<T>) endOfStream;
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
		deepRecycle(value);
		value = null;

		endOfStream = CompleteStage.ofException(e);
	}
}
