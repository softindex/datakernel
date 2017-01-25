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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopServer;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.net.BlockingSocketServer;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Static utility methods pertaining to ConcurrentService. Creates ConcurrentService from some other type of instances.
 */
public final class ServiceAdapters {
	private final Logger logger = getLogger(this.getClass());

	private ServiceAdapters() {
	}

	private static CompletionCallback toCompletionCallback(final SettableFuture<?> future) {
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				future.set(null);
			}

			@Override
			protected void onException(Exception exception) {
				future.setException(exception);
			}
		};
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
				instance.getEventloop().execute(new Runnable() {
					@Override
					public void run() {
						instance.start(toCompletionCallback(future));
					}
				});
				return future;
			}

			@Override
			public ListenableFuture<?> stop(final EventloopService instance, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				instance.getEventloop().execute(new Runnable() {
					@Override
					public void run() {
						instance.stop(toCompletionCallback(future));
					}
				});
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
				instance.getEventloop().execute(new Runnable() {
					@Override
					public void run() {
						instance.close(new CompletionCallback() {
							@Override
							protected void onComplete() {
								future.set(null);
							}

							@Override
							protected void onException(Exception e) {
								future.setException(e);
							}
						});
					}
				});
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
		return new ServiceAdapter<BlockingService>() {
			@Override
			public ListenableFuture<?> start(final BlockingService service, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							service.start();
							future.set(null);
						} catch (Exception e) {
							future.setException(e);
						}
					}
				});
				return future;
			}

			@Override
			public ListenableFuture<?> stop(final BlockingService service, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							service.stop();
							future.set(null);
						} catch (Exception e) {
							future.setException(e);
						}
					}
				});
				return future;
			}
		};
	}

	public static ServiceAdapter<BlockingSocketServer> forBlockingSocketServer() {
		return new ServiceAdapter<BlockingSocketServer>() {
			@Override
			public ListenableFuture<?> start(final BlockingSocketServer service, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				try {
					service.start();
					future.set(null);
				} catch (Exception e) {
					future.setException(e);
				}
				return future;
			}

			@Override
			public ListenableFuture<?> stop(final BlockingSocketServer service, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							service.stop();
							future.set(null);
						} catch (Exception e) {
							future.setException(e);
						}
					}
				});
				return future;
			}
		};
	}

	/**
	 * Returns factory which transforms Timer to ConcurrentService. On starting it doing nothing, on stop it cancel timer.
	 */
	public static ServiceAdapter<Timer> forTimer() {
		return new ServiceAdapter<Timer>() {
			@Override
			public ListenableFuture<?> start(Timer instance, Executor executor) {
				return Futures.immediateFuture(null);
			}

			@Override
			public ListenableFuture<?> stop(Timer instance, Executor executor) {
				instance.cancel();
				return Futures.immediateFuture(null);
			}
		};
	}

	/**
	 * Returns factory which transforms ExecutorService to ConcurrentService. On starting it doing nothing, on stopping it shuts down ExecutorService.
	 */
	public static ServiceAdapter<ExecutorService> forExecutorService() {
		return new ServiceAdapter<ExecutorService>() {
			@Override
			public ListenableFuture<?> start(ExecutorService instance, Executor executor) {
				return Futures.immediateFuture(null);
			}

			@Override
			public ListenableFuture<?> stop(final ExecutorService executorService, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				executor.execute(new Runnable() {
					@Override
					public void run() {
						executorService.shutdown();
						future.set(null);
					}
				});
				return future;
			}
		};
	}

	/**
	 * Returns factory which transforms Closeable object to ConcurrentService. On starting it doing nothing, on stopping it close Closeable.
	 */
	public static ServiceAdapter<Closeable> forCloseable() {
		return new ServiceAdapter<Closeable>() {
			@Override
			public ListenableFuture<?> start(Closeable closeable, Executor executor) {
				return Futures.immediateFuture(null);
			}

			@Override
			public ListenableFuture<?> stop(final Closeable closeable, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							closeable.close();
							future.set(null);
						} catch (IOException e) {
							future.setException(e);
						}
					}
				});
				return future;
			}
		};
	}

	/**
	 * Returns factory which transforms DataSource object to ConcurrentService. On starting it checks connecting , on stopping it close DataSource.
	 */
	public static ServiceAdapter<DataSource> forDataSource() {
		return new ServiceAdapter<DataSource>() {
			@Override
			public ListenableFuture<?> start(final DataSource dataSource, Executor executor) {
				final SettableFuture<?> future = SettableFuture.create();
				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							Connection connection = dataSource.getConnection();
							connection.close();
							future.set(null);
						} catch (Exception e) {
							future.setException(e);
						}
					}
				});
				return future;
			}

			@Override
			public ListenableFuture<?> stop(final DataSource dataSource, Executor executor) {
				return Futures.immediateFuture(null);
			}
		};
	}

	public static <T> ServiceAdapter<T> immediateServiceAdapter() {
		return new ServiceAdapter<T>() {
			@Override
			public ListenableFuture<?> start(Object instance, Executor executor) {
				return Futures.immediateFuture(null);
			}

			@Override
			public ListenableFuture<?> stop(Object instance, Executor executor) {
				return Futures.immediateFuture(null);
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
