package io.datakernel.async;

import java.util.concurrent.CompletionStage;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class SettableStage<T> extends AbstractCompletionStage<T> {
	private boolean done;
	private T result = null;
	private Throwable error;

	private SettableStage() {
	}

	public static <T> SettableStage<T> create() {
		return new SettableStage<>();
	}

	public static <T> SettableStage<T> immediateStage(T value) {
		SettableStage<T> stage = new SettableStage<>();
		stage.setResult(value);
		return stage;
	}

	public static <T> SettableStage<T> immediateFailedStage(Throwable error) {
		SettableStage<T> stage = new SettableStage<>();
		stage.setError(error);
		return stage;
	}

	public void setResult(T result) {
		assert !isDone();
		done = true;
		if (next == null) {
			this.result = result;
		} else {
			complete(result);
		}
	}

	public void setError(Throwable error) {
		assert !isDone();
		done = true;
		if (next == null) {
			this.error = error;
		} else {
			completeExceptionally(error);
		}
	}

	@Override
	protected <X> CompletionStage<X> subscribe(NextCompletionStage<T, X> next) {
		if (isDone()) {
			if (this.next == null) {
				getCurrentEventloop().post(() -> {
					if (error == null)
						complete(result);
					else
						completeExceptionally(error);
					result = null;
					error = null;
				});
			}
		}
		return super.subscribe(next);
	}

	public boolean isDone() {
		return done;
	}

	@Override
	public String toString() {
		return "{" + (isDone() ? (error == null ? result : error.getMessage()) : "") + "}";
	}
}
