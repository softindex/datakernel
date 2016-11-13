package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;

public class AsyncRunnables {
	public static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();

	private AsyncRunnables() {
	}

	private static final class DoneState {
		boolean done;
	}

	public static AsyncRunnable timeout(final Eventloop eventloop, final long timestamp, final AsyncRunnable runnable) {
		return new AsyncRunnable() {
			@Override
			public void run(final CompletionCallback callback) {
				final DoneState state = new DoneState();
				runnable.run(new CompletionCallback() {
					@Override
					protected void onComplete() {
						if (!state.done) {
							state.done = true;
							callback.setComplete();
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

	public static AsyncRunnable runInSequence(final Eventloop eventloop, final AsyncRunnable... runnables) {
		return runInSequence(eventloop, asList(runnables));
	}

	public static AsyncRunnable runInSequence(final Eventloop eventloop, final Iterable<AsyncRunnable> runnables) {
		return new AsyncRunnable() {
			@Override
			public void run(CompletionCallback callback) {
				next(runnables.iterator(), callback);
			}

			void next(final Iterator<AsyncRunnable> iterator, final CompletionCallback callback) {
				if (iterator.hasNext()) {
					AsyncRunnable nextTask = iterator.next();
					final long tick = eventloop.tick();
					nextTask.run(new ForwardingCompletionCallback(callback) {
						@Override
						protected void onComplete() {
							if (eventloop.getTick() != tick)
								next(iterator, callback);
							else
								eventloop.post(new Runnable() {
									@Override
									public void run() {
										next(iterator, callback);
									}
								});
						}
					});
				} else {
					callback.setComplete();
				}
			}
		};
	}

	public static AsyncRunnable runInParallel(final Eventloop eventloop, final AsyncRunnable... runnables) {
		return runInParallel(eventloop, asList(runnables));
	}

	private static final class RunState {
		int pending;

		public RunState(int pending) {
			this.pending = pending;
		}
	}

	public static AsyncRunnable runInParallel(final Eventloop eventloop, final List<AsyncRunnable> runnables) {
		return new AsyncRunnable() {
			@Override
			public void run(final CompletionCallback callback) {
				final RunState state = new RunState(runnables.size());
				if (state.pending == 0) {
					callback.setComplete();
					return;
				}
				for (AsyncRunnable runnable : runnables) {
					runnable.run(new CompletionCallback() {
						@Override
						protected void onComplete() {
							if (--state.pending == 0) {
								callback.setComplete();
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
}
