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
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioServer;
import io.datakernel.eventloop.NioService;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.datakernel.async.AsyncCallbacks.waitAll;
import static io.datakernel.service.ConcurrentServices.parallelService;
import static io.datakernel.service.ConcurrentServices.sequentialService;
import static org.slf4j.LoggerFactory.getLogger;

public final class NioEventloopRunner implements ConcurrentService {
	private static final Logger logger = getLogger(NioEventloopRunner.class);

	private final NioEventloop eventloop;
	private final ThreadFactory threadFactory;

	private final List<NioService> nioServices = new ArrayList<>();
	private final List<NioServer> nioServers = new ArrayList<>();
	private final List<ConcurrentService> concurrentServices = new ArrayList<>();

	private Thread thread;
	private SettableFuture<Boolean> startFuture;
	private SettableFuture<Boolean> stopFuture;

	/**
	 * Creates a new instance of EventloopRunner with Eventloop and thread factory
	 *
	 * @param eventloop     eventloop which will execute task
	 * @param threadFactory thread which will execute eventloop
	 */
	public NioEventloopRunner(NioEventloop eventloop, ThreadFactory threadFactory) {
		this.eventloop = eventloop;
		this.threadFactory = threadFactory;
	}

	/**
	 * Creates a new instance of EventloopRunner with Eventloop and default thread factory
	 *
	 * @param eventloop eventloop which will execute task
	 */
	public NioEventloopRunner(NioEventloop eventloop) {
		this.eventloop = eventloop;
		this.threadFactory = Executors.defaultThreadFactory();
	}

	/**
	 * Adds the collection of services from argument to nioServices
	 *
	 * @param services list of services which will be added
	 * @return changed EventloopRunner
	 */
	public NioEventloopRunner addNioServices(Collection<? extends NioService> services) {
		this.nioServices.addAll(services);
		return this;
	}

	public NioEventloopRunner addNioServers(Collection<? extends NioServer> nioServers) {
		this.nioServers.addAll(nioServers);
		return this;
	}

	/**
	 * Adds the services from argument to nioServices
	 *
	 * @param services list of services which will be added
	 * @return changed EventloopRunner
	 */
	public NioEventloopRunner addNioServices(NioService... services) {
		return addNioServices(Arrays.asList(services));
	}

	public NioEventloopRunner addNioServers(NioServer... nioServers) {
		return addNioServers(Arrays.asList(nioServers));
	}

	/**
	 * Adds the concurrent service from argument to nioServices
	 *
	 * @param services list of services which will be added
	 * @return changed EventloopRunner
	 */
	public NioEventloopRunner addConcurrentServices(Collection<? extends ConcurrentService> services) {
		this.concurrentServices.addAll(services);
		return this;
	}

	/**
	 * Adds concurrent services from argument to nioServices
	 *
	 * @param services list of services which will be added
	 * @return changed EventloopRunner
	 */
	public NioEventloopRunner addConcurrentServices(ConcurrentService... services) {
		return addConcurrentServices(Arrays.asList(services));
	}

	private void startupNioServers() throws IOException {
		for (final NioServer nioServer : nioServers) {
			logger.info("NioServer {} starting", nioServer);
			nioServer.listen();
		}
	}

	private void shutdownNioServers() {
		for (final NioServer nioServer : nioServers) {
			logger.info("NioServer {} closing", nioServer);
			nioServer.close();
		}
	}

	private void startupNioServicesAsync(final CompletionCallback startupCallback) {
		final CompletionCallback callbackWaitAll = waitAll(nioServices.size(), new CompletionCallback() {
			@Override
			public void onComplete() {
				try {
					startupNioServers();
					startupCallback.onComplete();
				} catch (IOException e) {
					logger.error("Exception while starting services", e);
					startupCallback.onException(e);
				}
			}

			@Override
			public void onException(Exception e) {
				startupCallback.onException(e);
			}
		});
		eventloop.keepAlive(true);
		for (final NioService nioService : nioServices) {
			logger.info("NioService {} starting", nioService);
			nioService.start(new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("NioService {} started", nioService);
					callbackWaitAll.onComplete();
				}

				@Override
				public void onException(Exception exception) {
					logger.error("Exception while starting {}", nioService);
					callbackWaitAll.onException(exception);
				}
			});
		}
	}

	/**
	 * Stops all nioServices
	 *
	 * @param shutdownCallback callback which will be called after completing or after exception
	 */
	public void shutdownNioServicesAsync(final CompletionCallback shutdownCallback) {
		shutdownNioServers();
		final CompletionCallback callbackWaitAll = waitAll(nioServices.size(), shutdownCallback);
		for (final NioService nioService : nioServices) {
			logger.info("NioService {} stopping", nioService);
			nioService.stop(new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("NioService {} stopped", nioService);
					callbackWaitAll.onComplete();
				}

				@Override
				public void onException(Exception exception) {
					logger.error("Exception while stopping {}", nioService);
					callbackWaitAll.onException(exception);
				}
			});
		}
		eventloop.keepAlive(false);
	}

	private void runInCurrentThread(final CompletionCallback startupCallback) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				startupNioServicesAsync(startupCallback);
			}
		});
		eventloop.run();
	}

	private void runInEventloopThread(final CompletionCallback shutdownCallback) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				shutdownNioServicesAsync(shutdownCallback);
			}
		});
	}

	synchronized private ListenableFuture<?> doStartAsync() {
		assert startFuture == null || stopFuture == null;
		if (startFuture != null)
			return startFuture;
		if (thread != null)
			return immediateFuture(false);
		if (stopFuture != null)
			return immediateFailedFuture(new IllegalStateException("Service is being stopped now"));
		this.startFuture = SettableFuture.create();

		thread = threadFactory.newThread(new Runnable() {
			@Override
			public void run() {
				runInCurrentThread(new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info("Starting {} complete", Arrays.toString(nioServices.toArray()));
						synchronized (NioEventloopRunner.this) {
							assert startFuture == null || stopFuture == null;
							if (startFuture != null) {
								startFuture.set(true);
								startFuture = null;
							}
						}
					}

					@Override
					public void onException(final Exception exception) {
						logger.error("Exception while starting {}", Arrays.toString(nioServices.toArray()));
						shutdownNioServicesAsync(new CompletionCallback() {
							@Override
							public void onComplete() {
								completionException();
							}

							@Override
							public void onException(Exception exception) {
								completionException();
							}

							private void completionException() {
								synchronized (NioEventloopRunner.this) {
									assert startFuture == null || stopFuture == null;
									if (startFuture != null) {
										startFuture.setException(exception);
										startFuture = null;
									}
								}
							}
						});
					}
				});

				synchronized (NioEventloopRunner.this) {
					assert startFuture == null || stopFuture == null;
					NioEventloopRunner.this.thread = null;
					if (stopFuture != null) {
						stopFuture.set(true);
						stopFuture = null;
					}
				}
			}
		});
		thread.start();

		return startFuture;
	}

	synchronized private ListenableFuture<?> doStopAsync() {
		assert startFuture == null || stopFuture == null;
		if (thread == null)
			return immediateFuture(false);
		if (stopFuture != null)
			return stopFuture;
		if (startFuture != null)
			return immediateFailedFuture(new IllegalStateException("Service is being started now"));
		stopFuture = SettableFuture.create();

		runInEventloopThread(new CompletionCallback() {
			@Override
			public void onComplete() {
				stopFuture.set(true);
			}

			@Override
			public void onException(Exception exception) {
				stopFuture.setException(exception);
			}
		});
		return stopFuture;
	}

	private ConcurrentService getConcurrentService() {
		return sequentialService(parallelService(concurrentServices), new ConcurrentService() {
			@Override
			public ListenableFuture<?> startFuture() {
				return doStartAsync();
			}

			@Override
			public ListenableFuture<?> stopFuture() {
				return doStopAsync();
			}
		});
	}

	/**
	 * Starts all services by this EventloopRunner. Concurrent services and NioServices will be
	 * started according to their implicit dependencies
	 *
	 * @return ListenableFuture which will be resolved once the action is complete.
	 */
	@Override
	public ListenableFuture<?> startFuture() {
		return getConcurrentService().startFuture();
	}

	/**
	 * Stops all services by this EventloopRunner.
	 *
	 * @return ListenableFuture which will be resolved once the action is complete.
	 */
	@Override
	public ListenableFuture<?> stopFuture() {
		return getConcurrentService().stopFuture();
	}
}