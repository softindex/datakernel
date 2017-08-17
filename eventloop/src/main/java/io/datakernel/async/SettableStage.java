package io.datakernel.async;

import java.util.concurrent.CompletionStage;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class SettableStage<T> extends AbstractCompletionStage<T> {
	private static final Object NO_RESULT = new Object();

	private T result = (T) NO_RESULT;
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
		if (next == null) {
			this.result = result;
		} else {
			complete(result);
		}
	}

	public void setError(Throwable error) {
		if (next == null) {
			this.error = error;
		} else {
			completeExceptionally(error);
		}
	}

	@Override
	protected <X> CompletionStage<X> subscribe(NextCompletionStage<T, X> next) {
		if (result != NO_RESULT || error != null) {
			if (this.next == null) {
				getCurrentEventloop().post(() -> {
					if (result != NO_RESULT)
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
}
