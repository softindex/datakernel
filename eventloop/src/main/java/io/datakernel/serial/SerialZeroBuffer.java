package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;

import static io.datakernel.util.Recyclable.deepRecycle;

public final class SerialZeroBuffer<T> implements SerialQueue<T>, Cancellable {
	private Exception exception;

	@Nullable
	private T value;

	private SettableStage<Void> put;
	private SettableStage<T> take;

	public boolean isWaiting() {
		return take != null || put != null;
	}

	public boolean isWaitingPut() {
		return put != null;
	}

	public boolean isWaitingTake() {
		return take != null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Stage<Void> put(@Nullable T value) {
		assert put == null;
		if (exception != null) return Stage.ofException(exception);

		if (take != null) {
			SettableStage<T> take = this.take;
			this.take = null;
			take.set(value);
			return Stage.complete();
		}

		this.value = value;
		this.put = new SettableStage<>();
		return put;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stage<T> take() {
		assert take == null;
		if (exception != null) return Stage.ofException(exception);

		if (put != null) {
			T value = this.value;
			SettableStage<Void> put = this.put;
			this.value = null;
			this.put = null;
			put.set(null);
			return Stage.of(value);
		}

		this.take = new SettableStage<>();
		return take;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void closeWithError(Throwable e) {
		if (exception != null) return;
		exception = e instanceof Exception ? (Exception) e : new RuntimeException(e);
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
	}

	@Nullable
	public Throwable getException() {
		return exception;
	}
}
