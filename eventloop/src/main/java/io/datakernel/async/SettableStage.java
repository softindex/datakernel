package io.datakernel.async;

import java.util.function.BiConsumer;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class SettableStage<T> extends AbstractStage<T> implements Callback<T> {
	private static final Object NO_RESULT = new Object();

	@SuppressWarnings("unchecked")
	protected T result = (T) NO_RESULT;
	protected Throwable exception;

	protected SettableStage() {
	}

	public static <T> SettableStage<T> create() {
		return new SettableStage<>();
	}

	@Override
	public void set(T result) {
		assert !isSet();
		if (next == null) {
			this.result = result;
		} else {
			this.result = null;
			complete(result);
		}
	}

	@Override
	public void setException(Throwable t) {
		assert !isSet();
		if (next == null) {
			this.result = null;
			this.exception = t;
		} else {
			this.result = null;
			completeExceptionally(t);
		}
	}

	public boolean trySet(T result) {
		if (isSet()) return false;
		set(result);
		return true;
	}

	public boolean trySetException(Throwable t) {
		if (isSet()) return false;
		setException(t);
		return true;
	}

	public boolean trySet(T result, Throwable throwable) {
		if (isSet()) return false;
		if (throwable == null) {
			trySet(result);
		} else {
			trySetException(throwable);
		}
		return true;
	}

	@Override
	protected void subscribe(BiConsumer<? super T, ? super Throwable> next) {
		if (isSet()) {
			if (this.next == null) { // to post only once
				getCurrentEventloop().post(() -> {
					if (exception == null) {
						complete(result);
					} else {
						completeExceptionally(exception);
					}

					result = null;
					exception = null;
				});
			}
		}
		super.subscribe(next);
	}

	public boolean isSet() {
		return result != NO_RESULT;
	}

	@Override
	public boolean isComplete() {
		return super.isComplete();
	}

	@Override
	public String toString() {
		return "{" + (isSet() ? (exception == null ? result : exception.getMessage()) : "") + "}";
	}
}
