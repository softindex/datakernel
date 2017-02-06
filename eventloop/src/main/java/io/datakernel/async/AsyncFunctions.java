package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.AsyncTimeoutException;

import java.util.Arrays;
import java.util.Iterator;

public class AsyncFunctions {
	public static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException();

	private AsyncFunctions() {
	}

	private static final class DoneState {
		boolean done;
	}

	public static <I, O> AsyncFunction<I, O> timeout(final Eventloop eventloop, final long timestamp, final AsyncFunction<I, O> function) {
		return new AsyncFunction<I, O>() {
			@Override
			public void apply(I input, final ResultCallback<O> callback) {
				final DoneState state = new DoneState();
				function.apply(input, new ResultCallback<O>() {
					@Override
					protected void onResult(O result) {
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
					eventloop.schedule(timestamp, new ScheduledRunnable() {
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

	public static <I, T, O> AsyncFunction<I, O> compose(AsyncFunction<T, O> g, AsyncFunction<I, T> f) {
		return pipeline(f, g);
	}

	public static <I, T, O> AsyncFunction<I, O> pipeline(final AsyncFunction<I, T> f, final AsyncFunction<T, O> g) {
		return new AsyncFunction<I, O>() {
			@Override
			public void apply(I input, final ResultCallback<O> callback) {
				f.apply(input, new ForwardingResultCallback<T>(callback) {
					@Override
					protected void onResult(T result) {
						g.apply(result, callback);
					}
				});
			}
		};
	}

	public static <I, O> AsyncFunction<I, O> pipeline(AsyncFunction<?, ?>... functions) {
		return pipeline(Arrays.asList(functions));
	}

	@SuppressWarnings("unchecked")
	public static <I, O> AsyncFunction<I, O> pipeline(final Iterable<AsyncFunction<?, ?>> functions) {
		return new AsyncFunction<I, O>() {
			@Override
			public void apply(I input, ResultCallback<O> callback) {
				next(input, (Iterator) functions.iterator(), callback);
			}

			private void next(Object item, final Iterator<AsyncFunction<Object, Object>> iterator, final ResultCallback<O> callback) {
				if (iterator.hasNext()) {
					AsyncFunction<Object, Object> next = iterator.next();
					next.apply(item, new ForwardingResultCallback<Object>(callback) {
						@Override
						protected void onResult(Object result) {
							next(result, iterator, callback);
						}
					});
				} else {
					callback.setResult((O) item);
				}
			}
		};
	}
}