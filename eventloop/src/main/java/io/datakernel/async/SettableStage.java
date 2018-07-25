package io.datakernel.async;

import io.datakernel.annotation.Nullable;

import java.util.function.BiConsumer;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

/**
 * Stage that can be completed or completedExceptionally manually.
 * <p>Can be used as root stage to start execution of chain of stages or when you want wrap your actions in {@code Stage}</p>
 *
 * @param <T> Result type
 */
public final class SettableStage<T> extends AbstractStage<T> implements Callback<T> {
	private static final Object NO_RESULT = new Object();

	@SuppressWarnings("unchecked")
	@Nullable
	protected T result = (T) NO_RESULT;

	@Nullable
	protected Throwable exception;

	public SettableStage() {
	}

	/**
	 * Sets the result of this {@code SettableStage} and completes it.
	 * <p>AssertionError is thrown when you try to set result for  already completed stage.</p>
	 */
	@Override
	public void set(@Nullable T result) {
		assert !isSet();
		if (next == null) {
			this.result = result;
		} else {
			this.result = null;
			complete(result);
		}
	}

	/**
	 * Sets exception and completes this {@code SettableStage} exceptionally.
	 * <p>AssertionError is thrown when you try to set exception for  already completed stage.</p>
	 *
	 * @param throwable exception
	 */
	@Override
	public void setException(Throwable throwable) {
		assert !isSet();
		if (next == null) {
			result = null;
			exception = throwable;
		} else {
			result = null;
			completeExceptionally(throwable);
		}
	}

	/**
	 * The same as {@link SettableStage#trySet(Object, Throwable)} )} but for result only.
	 */
	public boolean trySet(@Nullable T result) {
		if (isSet()) {
			return false;
		}
		set(result);
		return true;
	}

	/**
	 * Tries to set result or exception for this {@code SettableStage} if it not yet set.
	 * <p>Otherwise do nothing</p>
	 *
	 * @return {@code true} if result or exception was set, {@code false} otherwise
	 */
	public boolean trySet(@Nullable T result, @Nullable Throwable throwable) {
		if (isSet()) {
			return false;
		}
		if (throwable == null) {
			trySet(result);
		} else {
			trySetException(throwable);
		}
		return true;
	}

	/**
	 * The same as {@link SettableStage#trySet(Object, Throwable)} )} but for exception only.
	 */
	public boolean trySetException(Throwable throwable) {
		if (isSet()) {
			return false;
		}
		setException(throwable);
		return true;
	}

	public void post(@Nullable T result) {
		getCurrentEventloop().post(() -> set(result));
	}

	public void postException(Throwable throwable) {
		getCurrentEventloop().post(() -> setException(throwable));
	}

	public void post(@Nullable T result, @Nullable Throwable throwable) {
		getCurrentEventloop().post(() -> set(result, throwable));
	}

	public void tryPost(@Nullable T result) {
		getCurrentEventloop().post(() -> trySet(result));
	}

	public void tryPostException(Throwable throwable) {
		getCurrentEventloop().post(() -> trySetException(throwable));
	}

	public void tryPost(@Nullable T result, @Nullable Throwable throwable) {
		getCurrentEventloop().post(() -> trySet(result, throwable));
	}

	@Override
	protected void subscribe(BiConsumer<? super T, Throwable> next) {
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

	/**
	 * @return {@code true} if this {@code SettableStage} result is not set, {@code false} otherwise.
	 */
	public boolean isSet() {
		return result != NO_RESULT;
	}

	public boolean isSetResult() {
		return isSet() && exception == null;
	}

	public boolean isSetException() {
		return isSet() && exception != null;
	}

	public T getResult() {
		assert isSetResult();
		return result;
	}

	public Throwable getException() {
		assert isSetException();
		return exception;
	}

	@Override
	public String toString() {
		return "SettableStage{" + String.valueOf(isSet() ?
				(exception == null ?
						"result=" + result :
						"exception=" + exception.getClass().getSimpleName()) :
				"<uncomplete>") + '}';
	}
}
