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

package io.datakernel.guice.servicegraph;

import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioServer;
import io.datakernel.eventloop.NioService;
import io.datakernel.service.ConcurrentService;
import io.datakernel.service.ConcurrentServiceCallback;
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

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Static utility methods pertaining to ConcurrentService. Creates ConcurrentService from some other type of instances.
 */
public final class ServiceGraphFactories {
	private static final Logger logger = getLogger(ServiceGraphFactories.class);

	private ServiceGraphFactories() {
	}

	public static ServiceGraphFactory<ConcurrentService> factoryForConcurrentService() {
		return new ServiceGraphFactory<ConcurrentService>() {
			@Override
			public ConcurrentService getService(ConcurrentService service, Executor executor) {
				return service;
			}
		};
	}

	public static ServiceGraphFactory<NioService> factoryForNioService() {
		return new ServiceGraphFactory<NioService>() {

			@Override
			public ConcurrentService getService(final NioService node, Executor executor) {
				return new ConcurrentService() {
					@Override
					public void start(final ConcurrentServiceCallback callback) {
						node.getNioEventloop().postConcurrently(new Runnable() {
							@Override
							public void run() {
								node.start(callback);
								callback.onComplete();
							}
						});
					}

					@Override
					public void stop(final ConcurrentServiceCallback callback) {
						node.getNioEventloop().postConcurrently(new Runnable() {
							@Override
							public void run() {
								node.stop(callback);
								callback.onComplete();
							}
						});
					}
				};
			}
		};
	}

	public static ServiceGraphFactory<NioServer> factoryForNioServer() {
		return new ServiceGraphFactory<NioServer>() {
			@Override
			public ConcurrentService getService(final NioServer node, Executor executor) {
				return new ConcurrentService() {
					@Override
					public void start(final ConcurrentServiceCallback callback) {
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
					public void stop(final ConcurrentServiceCallback callback) {
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

	public static ServiceGraphFactory<NioEventloop> factoryForNioEventloop() {
		return new ServiceGraphFactory<NioEventloop>() {
			@Override
			public ConcurrentService getService(final NioEventloop node, final Executor executor) {
				return new ConcurrentService() {
					@Override
					public void start(final ConcurrentServiceCallback callback) {
						// TODO (vsavchuk) executor or threadFactory?
						executor.execute(new Runnable() {
							@Override
							public void run() {

								node.keepAlive(true);
								callback.onComplete();
								node.run();
							}
						});
					}

					@Override
					public void stop(final ConcurrentServiceCallback callback) {
						node.postConcurrently(new Runnable() {
							@Override
							public void run() {
								callback.onComplete();
								node.keepAlive(false);
							}
						});
					}
				};
			}
		};
	}

	/**
	 * Returns factory which transforms blocking Service to asynchronous non-blocking ConcurrentService. It runs blocking operations from other thread from executor.
	 */
	public static ServiceGraphFactory<Service> factoryForBlockingService() {
		return new ServiceGraphFactory<Service>() {
			@Override
			public ConcurrentService getService(final Service service, final Executor executor) {
				return new ConcurrentService() {
					@Override
					public void start(final ConcurrentServiceCallback callback) {
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
					public void stop(final ConcurrentServiceCallback callback) {
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
	public static ServiceGraphFactory<Timer> factoryForTimer() {
		return new ServiceGraphFactory<Timer>() {
			@Override
			public ConcurrentService getService(final Timer timer, Executor executor) {
				return new ConcurrentService() {
					@Override
					public void start(ConcurrentServiceCallback callback) {
						callback.onComplete();
					}

					@Override
					public void stop(ConcurrentServiceCallback callback) {
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
	public static ServiceGraphFactory<ExecutorService> factoryForExecutorService() {
		return new ServiceGraphFactory<ExecutorService>() {
			@Override
			public ConcurrentService getService(final ExecutorService executorService, Executor executor) {
				return new ConcurrentService() {
					@Override
					public void start(ConcurrentServiceCallback callback) {
						callback.onComplete();
					}

					@Override
					public void stop(ConcurrentServiceCallback callback) {
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
	public static ServiceGraphFactory<Closeable> factoryForCloseable() {
		return new ServiceGraphFactory<Closeable>() {
			@Override
			public ConcurrentService getService(final Closeable closeable, final Executor executor) {
				return new ConcurrentService() {
					@Override
					public void start(ConcurrentServiceCallback callback) {
						callback.onComplete();
					}

					@Override
					public void stop(final ConcurrentServiceCallback callback) {
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
	public static ServiceGraphFactory<DataSource> factoryForDataSource() {
		return new ServiceGraphFactory<DataSource>() {
			@Override
			public ConcurrentService getService(final DataSource dataSource, final Executor executor) {
				return new ConcurrentService() {
					@Override
					public void start(final ConcurrentServiceCallback callback) {
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
					public void stop(final ConcurrentServiceCallback callback) {
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
