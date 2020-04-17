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

package io.datakernel.service;

import org.slf4j.Logger;

import javax.sql.DataSource;
import java.io.Closeable;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;

import static org.slf4j.LoggerFactory.getLogger;

@SuppressWarnings("WeakerAccess")
public final class ServiceAdapters {
	private static final Logger logger = getLogger(ServiceAdapters.class);

	public static abstract class SimpleServiceAdapter<S> implements ServiceAdapter<S> {
		private final boolean startConcurrently;
		private final boolean stopConcurrently;

		protected SimpleServiceAdapter(boolean startConcurrently, boolean stopConcurrently) {
			this.startConcurrently = startConcurrently;
			this.stopConcurrently = stopConcurrently;
		}

		protected SimpleServiceAdapter() {
			this(true, true);
		}

		protected abstract void start(S instance) throws Exception;

		protected abstract void stop(S instance) throws Exception;

		@Override
		public final CompletableFuture<?> start(S instance, Executor executor) {
			CompletableFuture<?> future = new CompletableFuture<>();
			(startConcurrently ? executor : (Executor) Runnable::run).execute(() -> {
				try {
					start(instance);
					future.complete(null);
				} catch (Exception e) {
					future.completeExceptionally(e);
				}
			});
			return future;
		}

		@Override
		public final CompletableFuture<?> stop(S instance, Executor executor) {
			CompletableFuture<?> future = new CompletableFuture<>();
			(stopConcurrently ? executor : (Executor) Runnable::run).execute(() -> {
				try {
					stop(instance);
					future.complete(null);
				} catch (Exception e) {
					future.completeExceptionally(e);
				}
			});
			return future;
		}
	}

	public static ServiceAdapter<Service> forService() {
		return new ServiceAdapter<Service>() {
			@Override
			public CompletableFuture<?> start(Service instance, Executor executor) {
				return instance.start();
			}

			@Override
			public CompletableFuture<?> stop(Service instance, Executor executor) {
				return instance.stop();
			}
		};
	}

	/**
	 * Returns factory which transforms blocking Service to asynchronous non-blocking CompletableFuture. It runs blocking operations from other thread from
	 * executor.
	 */
	public static ServiceAdapter<BlockingService> forBlockingService() {
		return new SimpleServiceAdapter<BlockingService>() {
			@Override
			protected void start(BlockingService instance) throws Exception {
				instance.start();
			}

			@Override
			protected void stop(BlockingService instance) throws Exception {
				instance.stop();
			}
		};
	}

	/**
	 * Returns factory which transforms Timer to CompletableFuture. On starting it doing nothing, on stop it cancel timer.
	 */
	public static ServiceAdapter<Timer> forTimer() {
		return new SimpleServiceAdapter<Timer>(false, false) {
			@Override
			protected void start(Timer instance) {
			}

			@Override
			protected void stop(Timer instance) {
				instance.cancel();
			}
		};
	}

	/**
	 * Returns factory which transforms ExecutorService to CompletableFuture. On starting it doing nothing, on stopping it shuts down ExecutorService.
	 */
	public static ServiceAdapter<ExecutorService> forExecutorService() {
		return new SimpleServiceAdapter<ExecutorService>(false, true) {
			@Override
			protected void start(ExecutorService instance) {
			}

			@Override
			protected void stop(ExecutorService instance) throws Exception {
				List<Runnable> runnables = instance.shutdownNow();
				if (!runnables.isEmpty()) {
					logger.warn("Cancelled tasks: " + runnables);
				}
				if (!instance.isTerminated()) {
					logger.warn("Awaiting termination of " + instance + " ...");
					instance.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
				}
			}
		};
	}

	/**
	 * Returns factory which transforms Closeable object to CompletableFuture. On starting it doing nothing, on stopping it close Closeable.
	 */
	public static ServiceAdapter<Closeable> forCloseable() {
		return new SimpleServiceAdapter<Closeable>(false, true) {
			@Override
			protected void start(Closeable instance) {
			}

			@Override
			protected void stop(Closeable instance) throws Exception {
				instance.close();
			}
		};
	}

	/**
	 * Returns factory which transforms DataSource object to CompletableFuture. On starting it checks connecting , on stopping it close DataSource.
	 */
	public static ServiceAdapter<DataSource> forDataSource() {
		return new SimpleServiceAdapter<DataSource>(true, false) {
			@Override
			protected void start(DataSource instance) throws Exception {
				Connection connection = instance.getConnection();
				connection.close();
			}

			@Override
			protected void stop(DataSource instance) {
			}
		};
	}

	public static <T> ServiceAdapter<T> immediateServiceAdapter() {
		return new SimpleServiceAdapter<T>(false, false) {
			@Override
			protected void start(T instance) {
			}

			@Override
			protected void stop(T instance) {
			}
		};
	}

	@SafeVarargs
	public static <T, S extends ServiceAdapter<? super T>> ServiceAdapter<T> combinedAdapter(S... startOrder) {
		return combinedAdapter(Arrays.asList(startOrder));
	}

	public static <T> ServiceAdapter<T> combinedAdapter(List<? extends ServiceAdapter<? super T>> startOrder) {
		List<? extends ServiceAdapter<? super T>> stopOrder = new ArrayList<>(startOrder);
		Collections.reverse(stopOrder);
		return combinedAdapter(startOrder, stopOrder);
	}

	@FunctionalInterface
	private interface Action<T> {
		CompletableFuture<?> doAction(ServiceAdapter<T> serviceAdapter, T instance, Executor executor);
	}

	public static <T> ServiceAdapter<T> combinedAdapter(List<? extends ServiceAdapter<? super T>> startOrder,
			List<? extends ServiceAdapter<? super T>> stopOrder) {
		return new ServiceAdapter<T>() {
			@SuppressWarnings("unchecked")
			private void doAction(T instance, Executor executor,
					Iterator<? extends ServiceAdapter<? super T>> iterator, CompletableFuture<?> future,
					Action<T> action) {
				if (iterator.hasNext()) {
					action.doAction((ServiceAdapter<T>) iterator.next(), instance, executor)
							.whenCompleteAsync(($, e) -> {
								if (e == null) {
									doAction(instance, executor, iterator, future, action);
								} else if (e instanceof InterruptedException) {
									future.completeExceptionally(e);
								} else if (e instanceof ExecutionException) {
									future.completeExceptionally(e.getCause());
								}
							}, Runnable::run);
				} else {
					future.complete(null);
				}
			}

			@Override
			public CompletableFuture<Void> start(T instance, Executor executor) {
				CompletableFuture<Void> future = new CompletableFuture<>();
				doAction(instance, executor, startOrder.iterator(), future,
						ServiceAdapter::start);
				return future;
			}

			@Override
			public CompletableFuture<Void> stop(T instance, Executor executor) {
				CompletableFuture<Void> future = new CompletableFuture<>();
				doAction(instance, executor, stopOrder.iterator(), future,
						ServiceAdapter::stop);
				return future;
			}
		};
	}

}
