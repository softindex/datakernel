package io.datakernel.async;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class SettableStage<T> extends AbstractCompletionStage<T> {
	private static final Supplier NO_RESULT = () -> {throw new UnsupportedOperationException();};

	@SuppressWarnings("unchecked")
	protected Supplier<T> supplier = (Supplier<T>) NO_RESULT;
	protected Throwable exception;

	protected SettableStage() {
	}

	public static <T> SettableStage<T> create() {
		return new SettableStage<>();
	}

	public static <T> SettableStage<T> of(CompletionStage<T> stage) {
		SettableStage<T> settableStage = new SettableStage<>();
		settableStage.setStage(stage);
		return settableStage;
	}

	public void set(T result) {
		assert !isSet();
		if (next == null) {
			this.supplier = () -> result;
		} else {
			this.supplier = null;
			complete(result);
		}
	}

	public void setException(Throwable t) {
		assert !isSet();
		if (next == null) {
			this.supplier = null;
			this.exception = t;
		} else {
			this.supplier = null;
			completeExceptionally(t);
		}
	}

	public void set(T result, Throwable throwable) {
		if (throwable == null) {
			set(result);
		} else {
			setException(throwable);
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
			set(result);
		} else {
			setException(throwable);
		}
		return true;
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

	public void trySetStage(CompletionStage<T> stage) {
		stage.whenComplete((t, throwable) -> {
			if (throwable == null) {
				trySet(t);
			} else {
				trySetException(throwable);
			}
		});
	}

	@Override
	protected <X> CompletionStage<X> subscribe(NextCompletionStage<T, X> next) {
		if (isSet()) {
			if (this.next == null) {
				getCurrentEventloop().post(() -> {
					if (exception == null) {
						complete(supplier.get());
					} else {
						completeExceptionally(exception);
					}

					supplier = null;
					exception = null;
				});
			}
		}
		return super.subscribe(next);
	}

	public boolean isSet() {
		return supplier != NO_RESULT;
	}

	@Override
	public boolean isComplete() {
		return super.isComplete();
	}

	@Override
	public String toString() {
		return "{" + (isSet() ? (exception == null ? supplier : exception.getMessage()) : "") + "}";
	}
}
