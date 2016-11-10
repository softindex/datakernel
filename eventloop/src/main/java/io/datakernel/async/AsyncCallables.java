package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;

import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;

public class AsyncCallables {
	public static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();

	private AsyncCallables() {
	}

	public static <T> AsyncCallable<T> timeout(final Eventloop eventloop, final long timestamp, final AsyncCallable<T> callable) {
		return new AsyncCallable<T>() {
			boolean done;

			@Override
			public void call(final ResultCallback<T> callback) {
				final long tick = eventloop.getTick();
				eventloop.schedule(timestamp, new Runnable() {
					@Override
					public void run() {
						if (!done) {
							done = true;
							callback.setException(TIMEOUT_EXCEPTION);
						}
					}
				});
				callable.call(new ResultCallback<T>() {
					@Override
					protected void onResult(T result) {
						if (!done) {
							done = true;
							if (eventloop.getTick() != tick)
								callback.setResult(result);
							else
								callback.postResult(eventloop, result);
						}
					}

					@Override
					protected void onException(Exception e) {
						if (!done) {
							done = true;
							if (eventloop.getTick() != tick)
								callback.setException(e);
							else
								callback.postException(eventloop, e);
						}
					}
				});
			}
		};
	}

	public static <T> AsyncCallable<T[]> callAll(final Eventloop eventloop, final AsyncCallable<?>... callables) {
		return callAll(eventloop, asList(callables));
	}

	@SuppressWarnings("unchecked")
	public static <T> AsyncCallable<T[]> callAll(final Eventloop eventloop, final List<AsyncCallable<?>> callables) {
		return new AsyncCallable<T[]>() {
			int pending = callables.size();

			@Override
			public void call(final ResultCallback<T[]> callback) {
				final T[] results = (T[]) new Object[callables.size()];
				if (pending == 0) {
					callback.postResult(eventloop, results);
					return;
				}
				final long tick = eventloop.getTick();
				for (int i = 0; i < callables.size(); i++) {
					AsyncCallable<T> callable = (AsyncCallable<T>) callables.get(i);
					final int finalI = i;
					callable.call(new ResultCallback<T>() {
						@Override
						protected void onResult(T result) {
							results[finalI] = result;
							if (--pending == 0) {
								if (eventloop.getTick() != tick)
									callback.setResult(results);
								else
									callback.postResult(eventloop, results);
							}
						}

						@Override
						protected void onException(Exception e) {
							if (pending > 0) {
								pending = 0;
								if (eventloop.getTick() != tick)
									callback.setException(e);
								else
									callback.postException(eventloop, e);
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
			int pending = callables.size();

			@Override
			public void call(final ResultCallback<T[]> callback) {
				final T[] results = (T[]) new Object[callables.size()];
				if (pending == 0) {
					callback.postResult(eventloop, results);
					return;
				}
				eventloop.schedule(timestamp, new Runnable() {
					@Override
					public void run() {
						if (pending > 0) {
							pending = 0;
							callback.setResult(results);
						}
					}
				});
				final long tick = eventloop.getTick();
				for (int i = 0; i < callables.size(); i++) {
					final AsyncCallable<T> callable = (AsyncCallable<T>) callables.get(i);
					final int finalI = i;
					callable.call(new ResultCallback<T>() {
						@Override
						protected void onResult(T result) {
							results[finalI] = result;
							if (--pending == 0) {
								if (eventloop.getTick() != tick)
									callback.setResult(results);
								else
									callback.postResult(eventloop, results);
							}
						}

						@Override
						protected void onException(Exception e) {
							if (pending > 0) {
								pending = 0;
								if (eventloop.getTick() != tick)
									callback.setException(e);
								else
									callback.postException(eventloop, e);
							}
						}
					});
				}
			}
		};
	}
}
