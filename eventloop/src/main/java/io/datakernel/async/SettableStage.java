package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;

import java.util.concurrent.CompletionStage;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class SettableStage<T> extends AbstractCompletionStage<T> {
	private static final Object NO_RESULT = new Object();

	@SuppressWarnings("unchecked")
	private T value = (T) NO_RESULT;
	private Throwable exception;

	private SettableStage() {
	}

	public static <T> SettableStage<T> create() {
		return new SettableStage<>();
	}

	public static <T> SettableStage<T> immediateStage(T value) {
		SettableStage<T> stage = new SettableStage<>();
		stage.set(value);
		return stage;
	}

	public static <T> SettableStage<T> immediateFailedStage(Throwable t) {
		SettableStage<T> stage = new SettableStage<>();
		stage.setException(t);
		return stage;
	}

	public void set(T result) {
		assert !isSet();
		if (next == null) {
			this.value = result;
		} else {
			this.value = null;
			complete(result);
		}
	}

	public void setException(Throwable t) {
		assert !isSet();
		if (next == null) {
			this.value = null;
			this.exception = t;
		} else {
			this.value = null;
			completeExceptionally(t);
		}
	}

	public void postResult(Eventloop eventloop, T result) {
		eventloop.post(() -> set(result));
	}

	public void postError(Eventloop eventloop, Throwable error) {
		eventloop.post(() -> setException(error));
	}

	public void setStage(CompletionStage<T> stage) {
		stage.whenComplete((t, throwable) -> {
			if (throwable == null) {
				set(t);
			} else {
				setException(throwable);
			}
		});
	}

	@Override
	protected <X> CompletionStage<X> subscribe(NextCompletionStage<T, X> next) {
		if (isSet()) {
			if (this.next == null) {
				getCurrentEventloop().post(() -> {
					if (exception == null) complete(value);
					else completeExceptionally(exception);

					value = null;
					exception = null;
				});
			}
		}
		return super.subscribe(next);
	}

	public boolean isSet() {
		return value != NO_RESULT;
	}

	@Override
	public boolean isComplete() {
		return super.isComplete();
	}

	@Override
	public String toString() {
		return "{" + (isSet() ? (exception == null ? value : exception.getMessage()) : "") + "}";
	}
}
