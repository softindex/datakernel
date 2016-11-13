package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;

import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;

public class AsyncCallables {
	public static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();

	private AsyncCallables() {
	}

	private static final class DoneState {
		boolean done;
	}

	public static <T> AsyncCallable<T> timeout(final Eventloop eventloop, final long timestamp, final AsyncCallable<T> callable) {
		return new AsyncCallable<T>() {
			@Override
			public void call(final ResultCallback<T> callback) {
				final DoneState state = new DoneState();
				callable.call(new ResultCallback<T>() {
					@Override
					protected void onResult(T result) {
						if (!state.done) {
							state.done = true;
							callback.setResult(result);
						}
					}

					@Override
					protected void onException(Exception e) {
						if (!state.done) {
							state.done = true;
							callback.setException(e);
						}
					}
				});
				if (!state.done) {
					eventloop.schedule(timestamp, new Runnable() {
						@Override
						public void run() {
							if (!state.done) {
								state.done = true;
								callback.setException(TIMEOUT_EXCEPTION);
							}
						}
					});
				}
			}
		};
	}

	public static <T> AsyncCallable<T[]> callAll(final Eventloop eventloop, final AsyncCallable<?>... callables) {
		return callAll(eventloop, asList(callables));
	}

	private static final class CallState {
		int pending;

		public CallState(int pending) {
			this.pending = pending;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> AsyncCallable<T[]> callAll(final Eventloop eventloop, final List<AsyncCallable<?>> callables) {
		return new AsyncCallable<T[]>() {
			@Override
			public void call(final ResultCallback<T[]> callback) {
				final CallState state = new CallState(callables.size());
				final T[] results = (T[]) new Object[callables.size()];
				if (state.pending == 0) {
					callback.setResult(results);
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
								callback.setResult(results);
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

	public static <T> AsyncCallable<T[]> callWithTimeout(final Eventloop eventloop, final long timestamp, final AsyncCallable<? extends T>... callables) {
		return callWithTimeout(eventloop, timestamp, asList(callables));
	}

	public static <T> AsyncCallable<T[]> callWithTimeout(final Eventloop eventloop, final long timestamp, final List<AsyncCallable<? extends T>> callables) {
		return new AsyncCallable<T[]>() {
			@Override
			public void call(final ResultCallback<T[]> callback) {
				final CallState state = new CallState(callables.size());
				final T[] results = (T[]) new Object[callables.size()];
				if (state.pending == 0) {
					callback.setResult(results);
					return;
				}
				for (int i = 0; i < callables.size(); i++) {
					final AsyncCallable<T> callable = (AsyncCallable<T>) callables.get(i);
					final int finalI = i;
					callable.call(new ResultCallback<T>() {
						@Override
						protected void onResult(T result) {
							results[finalI] = result;
							if (--state.pending == 0) {
								callback.setResult(results);
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
				if (state.pending != 0) {
					eventloop.schedule(timestamp, new Runnable() {
						@Override
						public void run() {
							if (state.pending > 0) {
								state.pending = 0;
								callback.setResult(results);
							}
						}
					});
				}
			}
		};
	}
}
