package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.AsyncTimeoutException;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;

public class AsyncCallables {
	public static final AsyncTimeoutException CALLABLE_TIMEOUT_EXCEPTION = new AsyncTimeoutException("AsyncCallable timeout");

	private AsyncCallables() {
	}

	public static <T> AsyncCallable<T> callWithTimeout(final Eventloop eventloop, final long timestamp, final AsyncCallable<T> callable) {
		return new AsyncCallable<T>() {
			@Override
			public void call(final ResultCallback<T> callback) {
				final ScheduledRunnable scheduledRunnable = eventloop.schedule(timestamp, new ScheduledRunnable() {
					@Override
					public void run() {
						callback.setException(CALLABLE_TIMEOUT_EXCEPTION);
						if (callable instanceof AsyncCancellable) {
							((AsyncCancellable) callable).cancel();
						}
					}
				});

				callable.call(new ResultCallback<T>() {
					@Override
					protected void onResult(T result) {
						if (scheduledRunnable.isScheduledNow()) {
							scheduledRunnable.cancel();
							callback.setResult(result);
						}
					}

					@Override
					protected void onException(Exception e) {
						if (scheduledRunnable.isScheduledNow()) {
							scheduledRunnable.cancel();
							callback.setException(e);
						}
					}
				});
			}
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
		return new AsyncCallable<List<T>>() {
			@Override
			public void call(final ResultCallback<List<T>> callback) {
				final CallState state = new CallState(callables.size());
				final T[] results = (T[]) new Object[callables.size()];
				if (state.pending == 0) {
					callback.postResult(eventloop, Arrays.asList(results));
					return;
				}
				for (int i = 0; i < callables.size(); i++) {
					AsyncCallable<T> callable = (AsyncCallable<T>) callables.get(i);
					final int finalI = i;
					callable.call(new ResultCallback<T>() {
						@Override
						protected void onResult(T result) {
							results[finalI] = result;
							if (--state.pending == 0) {
								callback.setResult(Arrays.asList(results));
							}
						}

						@Override
						protected void onException(Exception e) {
							if (state.pending > 0) {
								state.pending = 0;
								callback.setException(e);
							}
						}
					});
				}
			}
		};
	}

	@SafeVarargs
	public static <T> AsyncCallable<List<T>> callAllWithTimeout(final Eventloop eventloop, final long timestamp, final AsyncCallable<T>... callables) {
		return callAllWithTimeout(eventloop, timestamp, asList(callables));
	}

	public static <T> AsyncCallable<List<T>> callAllWithTimeout(final Eventloop eventloop, final long timestamp, final List<? extends AsyncCallable<T>> callables) {
		return new AsyncCallable<List<T>>() {
			@Override
			public void call(final ResultCallback<List<T>> callback) {
				final CallState state = new CallState(callables.size());
				@SuppressWarnings("unchecked")
				final T[] results = (T[]) new Object[callables.size()];
				if (state.pending == 0) {
					callback.postResult(eventloop, Arrays.asList(results));
					return;
				}
				final ScheduledRunnable scheduledRunnable = eventloop.schedule(timestamp, new ScheduledRunnable() {
					@Override
					public void run() {
						state.pending = 0;
						callback.setResult(Arrays.asList(results));
					}
				});
				for (int i = 0; i < callables.size(); i++) {
					final AsyncCallable<T> callable = (AsyncCallable<T>) callables.get(i);
					final int finalI = i;
					callable.call(new ResultCallback<T>() {
						@Override
						protected void onResult(T result) {
							results[finalI] = result;
							if (--state.pending == 0) {
								scheduledRunnable.cancel();
								callback.setResult(Arrays.asList(results));
							}
						}

						@Override
						protected void onException(Exception e) {
							if (state.pending > 0) {
								state.pending = 0;
								scheduledRunnable.cancel();
								callback.setException(e);
							}
						}
					});
				}
			}
		};
	}
}
