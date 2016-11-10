/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.eventloop;

import io.datakernel.async.*;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class BlockingEventloopExecutor implements EventloopExecutor {
	private final Eventloop eventloop;
	private final Lock lock = new ReentrantLock();
	private final Condition notFull = lock.newCondition();
	private final AtomicInteger tasks = new AtomicInteger();

	private final int limit;

	// region builders
	private BlockingEventloopExecutor(Eventloop eventloop, int limit) {
		this.eventloop = eventloop;
		this.limit = limit;
	}

	public static BlockingEventloopExecutor create(Eventloop eventloop, int limit) {
		return new BlockingEventloopExecutor(eventloop, limit);
	}
	// endregion

	public int getLimit() {
		return limit;
	}

	private void post(final Runnable runnable) throws InterruptedException {
		lock.lock();
		try {
			while (tasks.get() > limit) {
				notFull.await();
			}
			tasks.incrementAndGet();
			eventloop.execute(runnable);
		} finally {
			lock.unlock();
		}
	}

	private void post(final Runnable runnable, ResultCallbackFuture<?> future) {
		try {
			post(runnable);
		} catch (InterruptedException e) {
			future.setException(e);
		}
	}

	private void complete() {
		lock.lock();
		try {
			tasks.decrementAndGet();
			notFull.signal();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void execute(final Runnable runnable) {
		try {
			post(new Runnable() {
				@Override
				public void run() {
					try {
						runnable.run();
					} finally {
						complete();
					}
				}
			});
		} catch (InterruptedException ignored) {
		}
	}

	public void execute(final AsyncRunnable asyncRunnable) {
		try {
			post(new Runnable() {
				@Override
				public void run() {
					asyncRunnable.run(new CompletionCallback() {
						@Override
						protected void onComplete() {
							complete();
						}

						@Override
						protected void onException(Exception exception) {
							complete();
						}
					});
				}
			});
		} catch (InterruptedException ignored) {
		}
	}

	@Override
	public Future<?> submit(Runnable runnable) {
		return submit(runnable, null);
	}

	@Override
	public Future<?> submit(AsyncRunnable asyncRunnable) {
		return submit(asyncRunnable, null);
	}

	@Override
	public <T> Future<T> submit(final Runnable runnable, final T result) {
		final ResultCallbackFuture<T> future = ResultCallbackFuture.create();
		post(new Runnable() {
			@Override
			public void run() {
				Exception exception = null;
				try {
					runnable.run();
				} catch (Exception e) {
					exception = e;
				}
				complete();
				if (exception == null) {
					future.setResult(result);
				} else {
					future.setException(exception);
				}
			}
		}, future);
		return future;
	}

	@Override
	public <T> Future<T> submit(final AsyncRunnable asyncRunnable, final T result) {
		final ResultCallbackFuture<T> future = ResultCallbackFuture.create();
		post(new Runnable() {
			@Override
			public void run() {
				asyncRunnable.run(new CompletionCallback() {
					@Override
					protected void onComplete() {
						setComplete();
						future.setResult(result);
					}

					@Override
					protected void onException(Exception exception) {
						setComplete();
						future.setException(exception);
					}
				});
			}
		}, future);
		return future;
	}

	@Override
	public <T> Future<T> submit(final Callable<T> callable) {
		final ResultCallbackFuture<T> future = ResultCallbackFuture.create();
		post(new Runnable() {
			@Override
			public void run() {
				T result = null;
				Exception exception = null;
				try {
					result = callable.call();
				} catch (Exception e) {
					exception = e;
				}
				complete();
				if (exception == null) {
					future.setResult(result);
				} else {
					future.setException(exception);
				}
			}
		}, future);
		return future;
	}

	@Override
	public <T> Future<T> submit(final AsyncCallable<T> asyncCallable) {
		final ResultCallbackFuture<T> future = ResultCallbackFuture.create();
		post(new Runnable() {
			@Override
			public void run() {
				asyncCallable.call(new ResultCallback<T>() {
					@Override
					protected void onResult(T result) {
						complete();
						future.setResult(result);
					}

					@Override
					protected void onException(Exception exception) {
						complete();
						future.setException(exception);
					}
				});
			}
		}, future);
		return future;
	}

}
