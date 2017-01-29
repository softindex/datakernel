package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.SimpleException;

import java.util.Collection;
import java.util.Iterator;

import static java.util.Arrays.asList;

public class AsyncRunnables {
	public static final SimpleException RUNNABLE_TIMEOUT_EXCEPTION = new SimpleException("AsyncRunnable timeout");

	private AsyncRunnables() {
	}

	public static AsyncRunnable timeout(final Eventloop eventloop, final long timestamp, final AsyncRunnable runnable) {
		return new AsyncRunnable() {
			@Override
			public void run(final CompletionCallback callback) {
				final ScheduledRunnable scheduledRunnable = eventloop.schedule(timestamp, new ScheduledRunnable() {
					@Override
					public void run() {
						callback.setException(RUNNABLE_TIMEOUT_EXCEPTION);
						if (runnable instanceof AsyncCancellable) {
							((AsyncCancellable) runnable).cancel();
						}
					}
				});

				runnable.run(new CompletionCallback() {
					@Override
					protected void onComplete() {
						if (scheduledRunnable.isScheduledNow()) {
							scheduledRunnable.cancel();
							callback.setComplete();
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

	public static AsyncRunnable runInSequence(final Eventloop eventloop, final AsyncRunnable... runnables) {
		return runInSequence(eventloop, asList(runnables));
	}

	public static AsyncRunnable runInSequence(final Eventloop eventloop, final Iterable<? extends AsyncRunnable> runnables) {
		return new AsyncRunnable() {
			@Override
			public void run(CompletionCallback callback) {
				next(runnables.iterator(), callback);
			}

			void next(final Iterator<? extends AsyncRunnable> iterator, final CompletionCallback callback) {
				if (iterator.hasNext()) {
					AsyncRunnable nextTask = iterator.next();
					final long microTick = eventloop.getMicroTick();
					nextTask.run(new ForwardingCompletionCallback(callback) {
						@Override
						protected void onComplete() {
							if (eventloop.getMicroTick() != microTick)
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

	public static AsyncRunnable runInParallel(final Eventloop eventloop, final Collection<? extends AsyncRunnable> runnables) {
		return new AsyncRunnable() {
			@Override
			public void run(final CompletionCallback callback) {
				final RunState state = new RunState(runnables.size());
				if (state.pending == 0) {
					callback.postComplete(eventloop);
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
