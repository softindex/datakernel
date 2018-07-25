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

import io.datakernel.async.AsyncSupplier;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
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

	private void post(Runnable runnable) throws InterruptedException {
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

	private void post(Runnable runnable, CompletableFuture<?> future) {
		try {
			post(runnable);
		} catch (InterruptedException e) {
			future.completeExceptionally(e);
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
	public void execute(Runnable runnable) {
		try {
			post(() -> {
				try {
					runnable.run();
				} finally {
					complete();
				}
			});
		} catch (InterruptedException ignored) {
		}
	}

	@Override
	public CompletableFuture<Void> submit(Runnable runnable) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		post(() -> {
			Exception exception = null;
			try {
				runnable.run();
			} catch (Exception e) {
				exception = e;
			}
			complete();
			if (exception == null) {
				future.complete(null);
			} else {
				future.completeExceptionally(exception);
			}
		}, future);
		return future;
	}

	@Override
	public <T> CompletableFuture<T> submit(Callable<T> callable) {
		CompletableFuture<T> future = new CompletableFuture<>();
		post(() -> {
			T result = null;
			Exception exception = null;
			try {
				result = callable.call();
			} catch (Exception e) {
				exception = e;
			}
			complete();
			if (exception == null) {
				future.complete(result);
			} else {
				future.completeExceptionally(exception);
			}
		}, future);
		return future;
	}

	@Override
	public <T> CompletableFuture<T> submit(AsyncSupplier<T> asyncCallable) {
		CompletableFuture<T> future = new CompletableFuture<>();
		post(() -> asyncCallable.get().whenComplete((t, throwable) -> complete()).whenComplete((t, throwable) -> {
			if (throwable == null) {
				future.complete(t);
			} else {
				future.completeExceptionally(throwable);
			}
		}), future);
		return future;
	}

}
