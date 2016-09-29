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
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.util.Timer;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

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
			public Service toService(Service instance, Executor executor) {
				return instance;
			}
		};
	}

	public static ServiceAdapter<EventloopService> forEventloopService() {
		return new ServiceAdapter<EventloopService>() {
			@Override
			public Service toService(final EventloopService instance, Executor executor) {
				return new Service() {
					@Override
					public ListenableFuture<?> start() {
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
					public ListenableFuture<?> stop() {
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
		};
	}

	public static ServiceAdapter<EventloopServer> forEventloopServer() {
		return new ServiceAdapter<EventloopServer>() {
			@Override
			public Service toService(final EventloopServer instance, Executor executor) {
				return new Service() {
					@Override
					public ListenableFuture<?> start() {
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
					public ListenableFuture<?> stop() {
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
		};
	}

	public static ServiceAdapter<Eventloop> forEventloop(final ThreadFactory threadFactory) {
		return new ServiceAdapter<Eventloop>() {
			@Override
			public Service toService(final Eventloop eventloop, final Executor executor) {
				return new Service() {
					volatile SettableFuture<?> stopFuture;

					@Override
					public ListenableFuture<?> start() {
						final SettableFuture<?> future = SettableFuture.create();
						threadFactory.newThread(new Runnable() {
							@Override
							public void run() {
								eventloop.keepAlive(true);
								future.set(null);
								eventloop.run();
								if (stopFuture != null) {
									stopFuture.set(null);
								}
							}
						}).start();
						return future;
					}

					@Override
					public ListenableFuture<?> stop() {
						stopFuture = SettableFuture.create();
						eventloop.execute(new Runnable() {
							@Override
							public void run() {
								eventloop.keepAlive(false);
							}
						});
						return stopFuture;
					}
				};
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
			public Service toService(final BlockingService service, final Executor executor) {
				return new Service() {
					@Override
					public ListenableFuture<?> start() {
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
					public ListenableFuture<?> stop() {
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
		};
	}

	/**
	 * Returns factory which transforms Timer to ConcurrentService. On starting it doing nothing, on stop it cancel timer.
	 */
	public static ServiceAdapter<Timer> forTimer() {
		return new ServiceAdapter<Timer>() {
			@Override
			public Service toService(final Timer instance, Executor executor) {
				return new Service() {
					@Override
					public ListenableFuture<?> start() {
						return Futures.immediateFuture(null);
					}

					@Override
					public ListenableFuture<?> stop() {
						instance.cancel();
						return Futures.immediateFuture(null);
					}
				};
			}
		};
	}

	/**
	 * Returns factory which transforms ExecutorService to ConcurrentService. On starting it doing nothing, on stopping it shuts down ExecutorService.
	 */
	public static ServiceAdapter<ExecutorService> forExecutorService() {
		return new ServiceAdapter<ExecutorService>() {
			@Override
			public Service toService(final ExecutorService executorService, final Executor executor) {
				return new Service() {
					@Override
					public ListenableFuture<?> start() {
						return Futures.immediateFuture(null);
					}

					@Override
					public ListenableFuture<?> stop() {
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
		};
	}

	/**
	 * Returns factory which transforms Closeable object to ConcurrentService. On starting it doing nothing, on stopping it close Closeable.
	 */
	public static ServiceAdapter<Closeable> forCloseable() {
		return new ServiceAdapter<Closeable>() {
			@Override
			public Service toService(final Closeable closeable, final Executor executor) {
				return new Service() {
					@Override
					public ListenableFuture<?> start() {
						return Futures.immediateFuture(null);
					}

					@Override
					public ListenableFuture<?> stop() {
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
		};
	}

	/**
	 * Returns factory which transforms DataSource object to ConcurrentService. On starting it checks connecting , on stopping it close DataSource.
	 */
	public static ServiceAdapter<DataSource> forDataSource() {
		return new ServiceAdapter<DataSource>() {
			@Override
			public Service toService(final DataSource dataSource, final Executor executor) {
				return new Service() {
					@Override
					public ListenableFuture<?> start() {
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
					public ListenableFuture<?> stop() {
						final SettableFuture<?> future = SettableFuture.create();
						if (!(dataSource instanceof Closeable)) {
							return Futures.immediateFuture(null);
						}
						executor.execute(new Runnable() {
							@Override
							public void run() {
								try {
									((Closeable) dataSource).close();
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
		};
	}
}
