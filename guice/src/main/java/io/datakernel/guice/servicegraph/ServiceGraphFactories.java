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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.datakernel.service.ConcurrentService;
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

import static com.google.common.util.concurrent.Futures.immediateFuture;
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

	/**
	 * Returns factory which transforms blocking Service to asynchronous non-blocking ConcurrentService. It runs blocking operations from other thread from executor.
	 */
	public static ServiceGraphFactory<Service> factoryForBlockingService() {
		return new ServiceGraphFactory<Service>() {
			@Override
			public ConcurrentService getService(final Service service, final Executor executor) {
				return new ConcurrentService() {
					@Override
					public ListenableFuture<?> startFuture() {
						final SettableFuture<Boolean> future = SettableFuture.create();
						executor.execute(new Runnable() {
							@Override
							public void run() {
								try {
									service.start();
									future.set(true);
								} catch (Exception e) {
									future.setException(e);
								}
							}
						});
						return future;
					}

					@Override
					public ListenableFuture<?> stopFuture() {
						final SettableFuture<Boolean> future = SettableFuture.create();
						executor.execute(new Runnable() {
							@Override
							public void run() {
								try {
									service.stop();
									future.set(true);
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
	public static ServiceGraphFactory<Timer> factoryForTimer() {
		return new ServiceGraphFactory<Timer>() {
			@Override
			public ConcurrentService getService(final Timer timer, Executor executor) {
				return new ConcurrentService() {
					@Override
					public ListenableFuture<?> startFuture() {
						return Futures.immediateFuture(true);
					}

					@Override
					public ListenableFuture<?> stopFuture() {
						timer.cancel();
						return Futures.immediateFuture(true);
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
					public ListenableFuture<?> startFuture() {
						return Futures.immediateFuture(true);
					}

					@Override
					public ListenableFuture<?> stopFuture() {
						List<Runnable> runnables = executorService.shutdownNow();
						for (Runnable runnable : runnables) {
							logger.warn("Remaining tasks {}", runnable);
						}
						return Futures.immediateFuture(true);
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
					public ListenableFuture<?> startFuture() {
						return immediateFuture(true);
					}

					@Override
					public ListenableFuture<?> stopFuture() {
						final SettableFuture<Boolean> future = SettableFuture.create();
						executor.execute(new Runnable() {
							@Override
							public void run() {
								try {
									closeable.close();
									future.set(true);
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
	public static ServiceGraphFactory<DataSource> factoryForDataSource() {
		return new ServiceGraphFactory<DataSource>() {
			@Override
			public ConcurrentService getService(final DataSource dataSource, final Executor executor) {
				return new ConcurrentService() {
					@Override
					public ListenableFuture<?> startFuture() {
						final SettableFuture<Boolean> future = SettableFuture.create();
						executor.execute(new Runnable() {
							@Override
							public void run() {
								try {
									Connection connection = dataSource.getConnection();
									connection.close();
									future.set(true);
								} catch (Exception e) {
									future.setException(e);
								}
							}
						});
						return future;
					}

					@Override
					public ListenableFuture<?> stopFuture() {
						if (dataSource instanceof Closeable) {
							final SettableFuture<Boolean> future = SettableFuture.create();
							executor.execute(new Runnable() {
								@Override
								public void run() {
									try {
										((Closeable) dataSource).close();
										future.set(true);
									} catch (IOException e) {
										future.setException(e);
									}
								}
							});
							return future;
						} else
							return immediateFuture(true);
					}
				};
			}
		};
	}
}
