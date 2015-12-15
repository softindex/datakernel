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

package io.datakernel.guice.boot;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioServer;
import io.datakernel.eventloop.NioService;
import io.datakernel.service.AsyncService;
import io.datakernel.service.AsyncServiceCallback;
import io.datakernel.service.Service;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Static utility methods pertaining to ConcurrentService. Creates ConcurrentService from some other type of instances.
 */
public final class AsyncServiceAdapters {
	private static final Logger logger = getLogger(AsyncServiceAdapters.class);

	private AsyncServiceAdapters() {
	}

	public static AsyncServiceAdapter<AsyncService> forAsyncService() {
		return new AsyncServiceAdapter<AsyncService>() {
			@Override
			public AsyncService toService(AsyncService service, Executor executor) {
				return service;
			}
		};
	}

	public static CompletionCallback toCompletionCallback(final AsyncServiceCallback callback) {
		return new CompletionCallback() {
			@Override
			public void onComplete() {
				callback.onComplete();
			}

			@Override
			public void onException(Exception exception) {
				callback.onException(exception);
			}
		};
	}

	public static AsyncServiceAdapter<NioService> forNioService() {
		return new AsyncServiceAdapter<NioService>() {

			@Override
			public AsyncService toService(final NioService node, Executor executor) {
				return new AsyncService() {
					@Override
					public void start(final AsyncServiceCallback callback) {
						node.getNioEventloop().postConcurrently(new Runnable() {
							@Override
							public void run() {
								node.start(toCompletionCallback(callback));
							}
						});
					}

					@Override
					public void stop(final AsyncServiceCallback callback) {
						node.getNioEventloop().postConcurrently(new Runnable() {
							@Override
							public void run() {
								node.stop(toCompletionCallback(callback));
							}
						});
					}
				};
			}
		};
	}

	public static AsyncServiceAdapter<NioServer> forNioServer() {
		return new AsyncServiceAdapter<NioServer>() {
			@Override
			public AsyncService toService(final NioServer node, Executor executor) {
				return new AsyncService() {
					@Override
					public void start(final AsyncServiceCallback callback) {
						node.getNioEventloop().postConcurrently(new Runnable() {
							@Override
							public void run() {
								try {
									node.listen();
									callback.onComplete();
								} catch (IOException e) {
									callback.onException(e);
								}
							}
						});
					}

					@Override
					public void stop(final AsyncServiceCallback callback) {
						node.getNioEventloop().postConcurrently(new Runnable() {
							@Override
							public void run() {
								node.close();
								callback.onComplete();
							}
						});
					}
				};
			}
		};
	}

	public static AsyncServiceAdapter<NioEventloop> forNioEventloop(final ThreadFactory threadFactory) {
		return new AsyncServiceAdapter<NioEventloop>() {
			@Override
			public AsyncService toService(final NioEventloop eventloop, final Executor executor) {
				return new AsyncService() {
					volatile AsyncServiceCallback stopCallback;

					@Override
					public void start(final AsyncServiceCallback callback) {
						threadFactory.newThread(new Runnable() {
							@Override
							public void run() {
								eventloop.keepAlive(true);
								callback.onComplete();
								eventloop.run();
								if (stopCallback != null) {
									stopCallback.onComplete();
								}
							}
						}).start();
					}

					@Override
					public void stop(final AsyncServiceCallback callback) {
						stopCallback = callback;
						eventloop.postConcurrently(new Runnable() {
							@Override
							public void run() {
								eventloop.keepAlive(false);
							}
						});
					}
				};
			}
		};
	}

	public static AsyncServiceAdapter<NioEventloop> forNioEventloop() {
		return forNioEventloop(new ThreadFactory() {
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
	public static AsyncServiceAdapter<Service> forBlockingService() {
		return new AsyncServiceAdapter<Service>() {
			@Override
			public AsyncService toService(final Service service, final Executor executor) {
				return new AsyncService() {
					@Override
					public void start(final AsyncServiceCallback callback) {
						executor.execute(new Runnable() {
							@Override
							public void run() {
								try {
									service.start();
									callback.onComplete();
								} catch (Exception e) {
									callback.onException(e);
								}
							}
						});
					}

					@Override
					public void stop(final AsyncServiceCallback callback) {
						executor.execute(new Runnable() {
							@Override
							public void run() {
								try {
									service.stop();
									callback.onComplete();
								} catch (Exception e) {
									callback.onException(e);
								}
							}
						});
					}
				};
			}
		};
	}

	/**
	 * Returns factory which transforms Timer to ConcurrentService. On starting it doing nothing, on stop it cancel timer.
	 */
	public static AsyncServiceAdapter<Timer> forTimer() {
		return new AsyncServiceAdapter<Timer>() {
			@Override
			public AsyncService toService(final Timer timer, Executor executor) {
				return new AsyncService() {
					@Override
					public void start(AsyncServiceCallback callback) {
						callback.onComplete();
					}

					@Override
					public void stop(AsyncServiceCallback callback) {
						timer.cancel();
						callback.onComplete();
					}
				};
			}
		};
	}

	/**
	 * Returns factory which transforms ExecutorService to ConcurrentService. On starting it doing nothing, on stopping it shuts down ExecutorService.
	 */
	public static AsyncServiceAdapter<ExecutorService> forExecutorService() {
		return new AsyncServiceAdapter<ExecutorService>() {
			@Override
			public AsyncService toService(final ExecutorService executorService, Executor executor) {
				return new AsyncService() {
					@Override
					public void start(AsyncServiceCallback callback) {
						callback.onComplete();
					}

					@Override
					public void stop(AsyncServiceCallback callback) {
						List<Runnable> runnables = executorService.shutdownNow();
						for (Runnable runnable : runnables) {
							logger.warn("Remaining tasks {}", runnable);
						}
						callback.onComplete();
					}
				};
			}
		};
	}

	/**
	 * Returns factory which transforms Closeable object to ConcurrentService. On starting it doing nothing, on stopping it close Closeable.
	 */
	public static AsyncServiceAdapter<Closeable> forCloseable() {
		return new AsyncServiceAdapter<Closeable>() {
			@Override
			public AsyncService toService(final Closeable closeable, final Executor executor) {
				return new AsyncService() {
					@Override
					public void start(AsyncServiceCallback callback) {
						callback.onComplete();
					}

					@Override
					public void stop(final AsyncServiceCallback callback) {
						executor.execute(new Runnable() {
							@Override
							public void run() {
								try {
									closeable.close();
									callback.onComplete();
								} catch (IOException e) {
									callback.onException(e);
								}
							}
						});
					}
				};
			}
		};
	}

	/**
	 * Returns factory which transforms DataSource object to ConcurrentService. On starting it checks connecting , on stopping it close DataSource.
	 */
	public static AsyncServiceAdapter<DataSource> forDataSource() {
		return new AsyncServiceAdapter<DataSource>() {
			@Override
			public AsyncService toService(final DataSource dataSource, final Executor executor) {
				return new AsyncService() {
					@Override
					public void start(final AsyncServiceCallback callback) {
						executor.execute(new Runnable() {
							@Override
							public void run() {
								try {
									Connection connection = dataSource.getConnection();
									connection.close();
									callback.onComplete();
								} catch (Exception e) {
									callback.onException(e);
								}
							}
						});
					}

					@Override
					public void stop(final AsyncServiceCallback callback) {
						if (dataSource instanceof Closeable) {
							executor.execute(new Runnable() {
								@Override
								public void run() {
									try {
										((Closeable) dataSource).close();
										callback.onComplete();
									} catch (IOException e) {
										callback.onException(e);
									}
								}
							});
						} else
							callback.onComplete();
					}
				};
			}
		};
	}
}
