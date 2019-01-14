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

import io.datakernel.exception.UncheckedException;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
	public void execute(@NotNull Runnable runnable) {
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

	@NotNull
	@Override
	public CompletableFuture<Void> submit(@NotNull Runnable computation) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		post(() -> {
			try {
				computation.run();
			} catch (UncheckedException u) {
				future.completeExceptionally(u.getCause());
				return;
			}
			future.complete(null);
		}, future);
		return future;
	}

	@NotNull
	@Override
	public <T> CompletableFuture<T> submit(@NotNull Callable<T> computation) {
		CompletableFuture<T> future = new CompletableFuture<>();
		execute(() -> {
			T result;
			try {
				result = computation.call();
			} catch (UncheckedException u) {
				future.completeExceptionally(u.getCause());
				return;
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				future.completeExceptionally(e);
				return;
			}
			future.complete(result);
		});
		return future;
	}

	@NotNull
	@Override
	public <T> CompletableFuture<T> submit(@NotNull Consumer<BiConsumer<T, Throwable>> callbackConsumer) {
		CompletableFuture<T> future = new CompletableFuture<>();
		post(() -> {
			try {
				callbackConsumer.accept((result, e) -> {
					if (e == null) {
						future.complete(result);
					} else {
						future.completeExceptionally(e);
					}
				});
			} catch (UncheckedException u) {
				future.completeExceptionally(u.getCause());
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				future.completeExceptionally(e);
			}
		}, future);
		return future;
	}


}
