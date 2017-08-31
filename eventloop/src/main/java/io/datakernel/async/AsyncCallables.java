package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.AsyncTimeoutException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static java.util.Arrays.asList;

public class AsyncCallables {
	public static final AsyncTimeoutException CALLABLE_TIMEOUT_EXCEPTION = new AsyncTimeoutException("AsyncCallable timeout");

	private AsyncCallables() {
	}

	public static <T> AsyncCallable<T> callWithTimeout(final Eventloop eventloop, final long timestamp, final AsyncCallable<T> callable) {
		return () -> {
			final SettableStage<T> stage = SettableStage.create();
			final CompletionStage<T> stageCall = callable.call();
			final ScheduledRunnable scheduledRunnable = eventloop.schedule(timestamp, () -> {
				stage.setError(CALLABLE_TIMEOUT_EXCEPTION);
				Stages.tryCancel(stageCall);
			});

			stageCall.whenComplete((t, throwable) -> {
				if (scheduledRunnable.isComplete()) return;
				scheduledRunnable.cancel();
				AsyncCallbacks.forwardTo(stage, t, throwable);
			});

			return stage;
		};
	}

	@SafeVarargs
	public static <T> AsyncCallable<List<T>> callAll(final Eventloop eventloop, final AsyncCallable<T>... callables) {
		return callAll(eventloop, asList(callables));
	}

	private static final class CallState {
		int pending;

		public CallState(int pending) {
			this.pending = pending;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> AsyncCallable<List<T>> callAll(final Eventloop eventloop, final List<? extends AsyncCallable<T>> callables) {
		return () -> {
			final SettableStage<List<T>> stage = SettableStage.create();
			final CallState state = new CallState(callables.size());
			final T[] results = (T[]) new Object[callables.size()];
			if (state.pending == 0) {
				stage.postResult(eventloop, Arrays.asList(results));
				return stage;
			}

			for (int i = 0; i < callables.size(); i++) {
				AsyncCallable<T> callable = callables.get(i);
				final int finalI = i;
				callable.call().whenComplete((t, throwable) -> {
					if (throwable != null) {
						if (state.pending > 0) {
							state.pending = 0;
							stage.setError(throwable);
						}
					} else {
						results[finalI] = t;
						if (--state.pending == 0) {
							stage.setResult(Arrays.asList(results));
						}
					}
				});

			}
			return stage;
		};
	}

	@SafeVarargs
	public static <T> AsyncCallable<List<T>> callAllWithTimeout(final Eventloop eventloop, final long timestamp, final AsyncCallable<T>... callables) {
		return callAllWithTimeout(eventloop, timestamp, asList(callables));
	}

	public static <T> AsyncCallable<List<T>> callAllWithTimeout(final Eventloop eventloop, final long timestamp, final List<? extends AsyncCallable<T>> callables) {
		return () -> {
			final SettableStage<List<T>> stage = SettableStage.create();
			final CallState state = new CallState(callables.size());
			@SuppressWarnings("unchecked")
			final T[] results = (T[]) new Object[callables.size()];
			if (state.pending == 0) {
				stage.postResult(eventloop, Arrays.asList(results));
				return stage;
			}
			final ScheduledRunnable scheduledRunnable = eventloop.schedule(timestamp, () -> {
				state.pending = 0;
				stage.setResult(Arrays.asList(results));
			});

			for (int i = 0; i < callables.size(); i++) {
				final AsyncCallable<T> callable = callables.get(i);
				final int finalI = i;
				callable.call().whenComplete((t, throwable) -> {
					if (throwable != null) {
						if (state.pending > 0) {
							state.pending = 0;
							scheduledRunnable.cancel();
							stage.setError(throwable);
						}
					} else {
						results[finalI] = t;
						if (--state.pending == 0) {
							scheduledRunnable.cancel();
							stage.setResult(Arrays.asList(results));
						}
					}
				});
			}

			return stage;
		};
	}
}
