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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopServer;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.net.BlockingSocketServer;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * Static utility methods pertaining to ConcurrentService. Creates
 * ConcurrentService from some other type of instances.
 */
public final class ServiceAdapters {
	private ServiceAdapters() {
	}

	private static BiConsumer<Void, Throwable> completeFuture(SettableFuture<?> future) {
		return ($, throwable) -> {
			if (throwable != null) {
				future.setException(throwable);
			} else {
				future.set(null);
			}
		};
	}

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
		public final ListenableFuture<?> start(final S instance, Executor executor) {
			final SettableFuture<Void> future = SettableFuture.create();
			(startConcurrently ? executor : directExecutor()).execute(new Runnable() {
				@Override
				public void run() {
					try {
						start(instance);
						future.set(null);
					} catch (Exception e) {
						future.setException(e);
					}
				}
			});
			return future;
		}

		@Override
		public final ListenableFuture<?> stop(final S instance, Executor executor) {
			final SettableFuture<Void> future = SettableFuture.create();
			(stopConcurrently ? executor : directExecutor()).execute(new Runnable() {
				@Override
				public void run() {
					try {
						stop(instance);
						future.set(null);
					} catch (Exception e) {
						future.setException(e);
					}
				}
			});
			return future;
		}
	}

	public static ServiceAdapter<Service> forService() {
		return new ServiceAdapter<Service>() {
			@Override
			public ListenableFuture<?> start(Service instance, Executor executor) {
				return instance.start();
			}

			@Override
			public ListenableFuture<?> stop(Service instance, Executor executor) {
				return instance.stop();
			}
		};
	}

	public static ServiceAdapter<EventloopService> forEventloopService() {
		return new ServiceAdapter<EventloopService>() {
			@Override
			public ListenableFuture<?> start(final EventloopService instance, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				instance.getEventloop().execute(() -> instance.start().whenComplete(completeFuture(future)));
				return future;
			}

			@Override
			public ListenableFuture<?> stop(final EventloopService instance, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				instance.getEventloop().execute(() -> instance.stop().whenComplete(completeFuture(future)));
				return future;
			}
		};
	}

	public static ServiceAdapter<EventloopServer> forEventloopServer() {
		return new ServiceAdapter<EventloopServer>() {
			@Override
			public ListenableFuture<?> start(final EventloopServer instance, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				instance.getEventloop().execute(new Runnable() {
					@Override
					public void run() {
						try {
							instance.listen();
							future.set(null);
						} catch (IOException e) {
							future.setException(e);
						}
					}
				});
				return future;
			}

			@Override
			public ListenableFuture<?> stop(final EventloopServer instance, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				instance.getEventloop().execute(() -> instance.close().whenComplete(completeFuture(future)));
				return future;
			}
		};
	}

	public static ServiceAdapter<Eventloop> forEventloop(final ThreadFactory threadFactory) {
		return new ServiceAdapter<Eventloop>() {
			@Override
			public ListenableFuture<?> start(final Eventloop eventloop, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				threadFactory.newThread(new Runnable() {
					@Override
					public void run() {
						eventloop.keepAlive(true);
						future.set(null);
						eventloop.run();
					}
				}).start();
				return future;
			}

			@Override
			public ListenableFuture<?> stop(final Eventloop eventloop, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				final Thread eventloopThread = eventloop.getEventloopThread();
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						eventloop.keepAlive(false);
					}
				});
				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							eventloopThread.join();
							future.set(null);
						} catch (InterruptedException e) {
							future.setException(e);
						}
					}
				});
				return future;
			}
		};
	}

	public static ServiceAdapter<Eventloop> forEventloop() {
		return forEventloop(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread thread = Executors.defaultThreadFactory().newThread(r);
				thread.setName("eventloop: " + thread.getName());
				return thread;
			}
		});
	}

	/**
	 * Returns factory which transforms blocking Service to asynchronous non-blocking ConcurrentService. It runs blocking operations from other thread from executor.
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

	public static ServiceAdapter<BlockingSocketServer> forBlockingSocketServer() {
		return new SimpleServiceAdapter<BlockingSocketServer>() {
			@Override
			protected void start(BlockingSocketServer instance) throws Exception {
				instance.start();
			}

			@Override
			protected void stop(BlockingSocketServer instance) throws Exception {
				instance.stop();
			}
		};
	}

	/**
	 * Returns factory which transforms Timer to ConcurrentService. On starting it doing nothing, on stop it cancel timer.
	 */
	public static ServiceAdapter<Timer> forTimer() {
		return new SimpleServiceAdapter<Timer>(false, false) {
			@Override
			protected void start(Timer instance) throws Exception {
			}

			@Override
			protected void stop(Timer instance) throws Exception {
				instance.cancel();
			}
		};
	}

	/**
	 * Returns factory which transforms ExecutorService to ConcurrentService. On starting it doing nothing, on stopping it shuts down ExecutorService.
	 */
	public static ServiceAdapter<ExecutorService> forExecutorService() {
		return new SimpleServiceAdapter<ExecutorService>(false, true) {
			@Override
			protected void start(ExecutorService instance) throws Exception {
			}

			@Override
			protected void stop(ExecutorService instance) throws Exception {
				instance.shutdown();
			}
		};
	}

	/**
	 * Returns factory which transforms Closeable object to ConcurrentService. On starting it doing nothing, on stopping it close Closeable.
	 */
	public static ServiceAdapter<Closeable> forCloseable() {
		return new SimpleServiceAdapter<Closeable>(false, true) {
			@Override
			protected void start(Closeable instance) throws Exception {
			}

			@Override
			protected void stop(Closeable instance) throws Exception {
				instance.close();
			}
		};
	}

	/**
	 * Returns factory which transforms DataSource object to ConcurrentService. On starting it checks connecting , on stopping it close DataSource.
	 */
	public static ServiceAdapter<DataSource> forDataSource() {
		return new SimpleServiceAdapter<DataSource>(true, false) {
			@Override
			protected void start(DataSource instance) throws Exception {
				Connection connection = instance.getConnection();
				connection.close();
			}

			@Override
			protected void stop(DataSource instance) throws Exception {
			}
		};
	}

	public static <T> ServiceAdapter<T> immediateServiceAdapter() {
		return new SimpleServiceAdapter<T>(false, false) {
			@Override
			protected void start(T instance) throws Exception {
			}

			@Override
			protected void stop(T instance) throws Exception {
			}
		};
	}

	@SafeVarargs
	public static <T> ServiceAdapter<T> combinedAdapter(ServiceAdapter<? super T>... startOrder) {
		return combinedAdapter(Arrays.asList(startOrder));
	}

	public static <T> ServiceAdapter<T> combinedAdapter(final List<? extends ServiceAdapter<? super T>> startOrder) {
		List<? extends ServiceAdapter<? super T>> stopOrder = new ArrayList<>(startOrder);
		Collections.reverse(stopOrder);
		return combinedAdapter(startOrder, stopOrder);
	}

	private interface Action<T> {
		ListenableFuture<?> doAction(ServiceAdapter<? super T> serviceAdapter, T instance, Executor executor);
	}

	public static <T> ServiceAdapter<T> combinedAdapter(final List<? extends ServiceAdapter<? super T>> startOrder,
	                                                    final List<? extends ServiceAdapter<? super T>> stopOrder) {
		return new ServiceAdapter<T>() {
			private void doAction(final T instance, final Executor executor,
			                      final Iterator<? extends ServiceAdapter<? super T>> iterator, final SettableFuture<?> future,
			                      final Action<T> action) {
				if (iterator.hasNext()) {
					ServiceAdapter<? super T> next = iterator.next();
					final ListenableFuture<?> nextFuture = action.doAction(next, instance, executor);
					nextFuture.addListener(new Runnable() {
						@Override
						public void run() {
							try {
								nextFuture.get();
								doAction(instance, executor, iterator, future, action);
							} catch (InterruptedException e) {
								future.setException(e);
							} catch (ExecutionException e) {
								future.setException(e.getCause());
							}
						}
					}, directExecutor());
				} else {
					future.set(null);
				}
			}

			@Override
			public ListenableFuture<?> start(T instance, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				doAction(instance, executor, startOrder.iterator(), future, new Action<T>() {
					@Override
					public ListenableFuture<?> doAction(ServiceAdapter<? super T> serviceAdapter, T instance, Executor executor) {
						return serviceAdapter.start(instance, executor);
					}
				});
				return future;
			}

			@Override
			public ListenableFuture<?> stop(T instance, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				doAction(instance, executor, stopOrder.iterator(), future, new Action<T>() {
					@Override
					public ListenableFuture<?> doAction(ServiceAdapter<? super T> serviceAdapter, T instance, Executor executor) {
						return serviceAdapter.stop(instance, executor);
					}
				});
				return future;
			}
		};
	}
}
