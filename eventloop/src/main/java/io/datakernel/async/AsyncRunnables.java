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

	public static AsyncRunnable timeout(final Eventloop eventloop, final long timestamp, final AsyncRunnable runnable) {
		return new AsyncRunnable() {
			boolean done;

			@Override
			public void run(final CompletionCallback callback) {
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
				runnable.run(new CompletionCallback() {
					@Override
					protected void onComplete() {
						if (!done) {
							done = true;
							if (eventloop.getTick() != tick)
								callback.setComplete();
							else
								callback.postComplete(eventloop);
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

	public static AsyncRunnable runSequence(final Eventloop eventloop, final AsyncRunnable... runnables) {
		return runSequence(eventloop, asList(runnables));
	}

	public static AsyncRunnable runSequence(final Eventloop eventloop, final List<AsyncRunnable> runnables) {
		return new AsyncRunnable() {
			@Override
			public void run(CompletionCallback callback) {
				if (runnables.isEmpty()) {
					callback.postComplete(eventloop);
					return;
				}
				Iterator<AsyncRunnable> iterator = runnables.iterator();
				next(iterator, callback);
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
					callback.postComplete(eventloop);
				}
			}
		};
	}

	public static AsyncRunnable runParallel(final Eventloop eventloop, final AsyncRunnable... runnables) {
		return runParallel(eventloop, asList(runnables));
	}

	public static AsyncRunnable runParallel(final Eventloop eventloop, final List<AsyncRunnable> runnables) {
		return new AsyncRunnable() {
			int pending = runnables.size();

			@Override
			public void run(final CompletionCallback callback) {
				if (pending == 0) {
					callback.postComplete(eventloop);
					return;
				}
				final long tick = eventloop.getTick();
				for (AsyncRunnable runnable : runnables) {
					runnable.run(new CompletionCallback() {
						@Override
						protected void onComplete() {
							if (--pending == 0) {
								if (eventloop.getTick() != tick)
									callback.setComplete();
								else
									callback.postComplete(eventloop);
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
