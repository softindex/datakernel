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

	private int limit;

	private final CompletionCallback completionCallback = new CompletionCallback() {
		@Override
		protected void onComplete() {
			complete();
		}

		@Override
		protected void onException(Exception exception) {
			complete();
		}
	};

	public BlockingEventloopExecutor(Eventloop eventloop, int limit) {
		this.eventloop = eventloop;
		this.limit = limit;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
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
			future.fireException(e);
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

	public void execute(final AsyncTask asyncTask) {
		try {
			post(new Runnable() {
				@Override
				public void run() {
					asyncTask.execute(completionCallback);
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
	public Future<?> submit(AsyncTask asyncTask) {
		return submit(asyncTask, null);
	}

	@Override
	public <T> Future<T> submit(final Runnable runnable, final T result) {
		final ResultCallbackFuture<T> future = new ResultCallbackFuture<>();
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
					future.sendResult(result);
				} else {
					future.fireException(exception);
				}
			}
		}, future);
		return future;
	}

	@Override
	public <T> Future<T> submit(final AsyncTask asyncTask, final T result) {
		final ResultCallbackFuture<T> future = new ResultCallbackFuture<>();
		post(new Runnable() {
			@Override
			public void run() {
				asyncTask.execute(new CompletionCallback() {
					@Override
					protected void onComplete() {
						complete();
						future.sendResult(result);
					}

					@Override
					protected void onException(Exception exception) {
						complete();
						future.fireException(exception);
					}
				});
			}
		}, future);
		return future;
	}

	@Override
	public <T> Future<T> submit(final Callable<T> callable) {
		final ResultCallbackFuture<T> future = new ResultCallbackFuture<>();
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
					future.sendResult(result);
				} else {
					future.fireException(exception);
				}
			}
		}, future);
		return future;
	}

	@Override
	public <T> Future<T> submit(final AsyncCallable<T> asyncCallable) {
		final ResultCallbackFuture<T> future = new ResultCallbackFuture<>();
		post(new Runnable() {
			@Override
			public void run() {
				asyncCallable.call(new ResultCallback<T>() {
					@Override
					protected void onResult(T result) {
						complete();
						future.sendResult(result);
					}

					@Override
					protected void onException(Exception exception) {
						complete();
						future.fireException(exception);
					}
				});
			}
		}, future);
		return future;
	}

}
