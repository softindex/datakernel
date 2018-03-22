package io.datakernel.async;

import io.datakernel.annotation.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

/**
 * Stage that can be completed or completedExceptionally manually.
 * <p>Can be used as root stage to start execution of chain of stages or when you want wrap your actions in {@code Stage}</p>
 *
 * @param <T> Result type
 */
public final class SettableStage<T> extends AbstractStage<T> implements Callback<T> {
	private static final Object NO_RESULT = new Object();

	static final boolean DEBUG = false;
	static final List<Stage<?>> uncompleteStages = DEBUG ? new ArrayList<>() : null;

	@SuppressWarnings("unchecked")
	@Nullable
	protected T result = (T) NO_RESULT;
	protected Throwable exception;

	private final StacktracePrinter stacktrace;

	protected SettableStage(boolean complete) {
		if (DEBUG) {
			if (!complete) {
				uncompleteStages.add(this);
			}
			stacktrace = new StacktracePrinter();
		} else {
			stacktrace = null;
		}
	}

	public static <T> SettableStage<T> create() {
		return new SettableStage<>(false);
	}


	/**
	 * Sets the result of this {@code SettableStage} and completes it.
	 * <p>AssertionError is thrown when you try to set result for  already completed stage.</p>
	 */
	@Override
	public void set(@Nullable T result) {
		assert !isSet();
		if (DEBUG) {
			uncompleteStages.remove(this);
		}
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
	 * @param t exception
	 */
	@Override
	public void setException(@Nullable Throwable t) {
		assert !isSet();
		if (DEBUG) {
			uncompleteStages.remove(this);
		}
		if (next == null) {
			result = null;
			exception = t;
		} else {
			result = null;
			completeExceptionally(t);
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
	 * The same as {@link SettableStage#trySet(Object, Throwable)} )} but for exception only.
	 */
	public boolean trySetException(@Nullable Throwable t) {
		if (isSet()) {
			return false;
		}
		setException(t);
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

	@Override
	protected void subscribe(StageConsumer<? super T> next) {
		if (isSet()) {
			if (this.next == null) { // to post only once
				getCurrentEventloop().post(() -> {
					if (DEBUG) {
						uncompleteStages.remove(this);
					}

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

	public static void dumpUncompleteStages() {
		if (!DEBUG) {
			throw new UnsupportedOperationException("Dumping uncomplete stages is not possible when DEBUG is disabled!");
		}
		for (Stage<?> uncomplete : uncompleteStages) {
			System.err.println(uncomplete);
		}
	}

	@Override
	public String toString() {
		StringWriter writer = new StringWriter()
				.append("{")
				.append(String.valueOf(isSet() ? (exception == null ? result : exception.getClass().getSimpleName()) : "<uncomplete>"))
				.append("}");
		if (DEBUG) {
			writer.append(" Stacktrace:");
			stacktrace.printStackTrace(new PrintWriter(writer));
		}
		return writer.toString();
	}

	private static final class StacktracePrinter extends Throwable {

		public StacktracePrinter() {
			// strip constructor and creation method trace elements
			StackTraceElement[] stacktrace = getStackTrace();
			setStackTrace(Arrays.copyOfRange(stacktrace, 2, stacktrace.length));
		}

		@Override
		public String toString() {
			return "";
		}
	}
}
