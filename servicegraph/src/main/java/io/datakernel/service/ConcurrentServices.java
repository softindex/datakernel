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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioServer;
import io.datakernel.eventloop.NioService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ConcurrentServices {
	private ConcurrentServices() {
	}

	public static ConcurrentService immediateService() {
		return new ConcurrentService() {
			@Override
			public void startFuture(ConcurrentServiceCallback callback) {
				callback.onComplete();
			}

			@Override
			public void stopFuture(ConcurrentServiceCallback callback) {
				callback.onComplete();
			}
		};
	}

	public static ConcurrentService immediateFailedService(final Exception e) {
		return new ConcurrentService() {
			@Override
			public void startFuture(ConcurrentServiceCallback callback) {
				callback.onException(e);
			}

			@Override
			public void stopFuture(ConcurrentServiceCallback callback) {
				callback.onComplete();
			}
		};
	}

	public static ConcurrentService parallelService(ConcurrentService... callbacks) {
		return new ParallelService(Arrays.asList(callbacks));
	}

	public static ConcurrentService parallelService(List<? extends ConcurrentService> callbacks) {
		return new ParallelService(callbacks);
	}

	public static ConcurrentService sequentialService(ConcurrentService... callbacks) {
		return new SequentialService(Arrays.asList(callbacks));
	}

	public static ConcurrentService sequentialService(List<? extends ConcurrentService> callbacks) {
		return new SequentialService(callbacks);
	}

	public static ConcurrentService concurrentServiceOfNioService(final NioService nioService) {
		return concurrentServiceOfNioService(nioService, Executors.defaultThreadFactory());
	}

	public static ConcurrentService concurrentServiceOfNioService(final NioService nioService, final ThreadFactory threadFactory) {
		return new ConcurrentService() {
			@Override
			public void startFuture(final ConcurrentServiceCallback callback) {
				threadFactory.newThread(new Runnable() {
					@Override
					public void run() {
						nioService.getNioEventloop().post(new Runnable() {
							@Override
							public void run() {
								startupNioServiceAsync(nioService, callback);
							}
						});
						nioService.getNioEventloop().run();

					}
				}).start();
			}

			@Override
			public void stopFuture(final ConcurrentServiceCallback callback) {
				nioService.getNioEventloop().postConcurrently(new Runnable() {
					@Override
					public void run() {
						shutdownNioServiceAsync(nioService, callback);
					}
				});
			}
		};
	}

	public static ConcurrentService concurrentServiceOfNioServer(final NioServer nioServer) {
		return concurrentServiceOfNioServer(nioServer, Executors.defaultThreadFactory());
	}

	public static ConcurrentService concurrentServiceOfNioServer(final NioServer nioServer, final ThreadFactory threadFactory) {
		return concurrentServiceOfNioService(new NioService() {
			@Override
			public NioEventloop getNioEventloop() {
				return nioServer.getNioEventloop();
			}

			@Override
			public void start(CompletionCallback callback) {
				try {
					nioServer.listen();
					callback.onComplete();
				} catch (IOException e) {
					callback.onException(e);
				}
			}

			@Override
			public void stop(CompletionCallback callback) {
				nioServer.close();
				callback.onComplete();
			}
		}, threadFactory);
	}

	private static void startupNioServiceAsync(final NioService nioService, final CompletionCallback startupCallback) {
		nioService.getNioEventloop().keepAlive(true);
		nioService.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				startupCallback.onComplete();
			}

			@Override
			public void onException(Exception exception) {
				startupCallback.onException(exception);
			}
		});
	}

	public static void shutdownNioServiceAsync(final NioService nioService, final CompletionCallback shutdownCallback) {
		final CompletionCallback callbackWaitAll = AsyncCallbacks.waitAll(1, shutdownCallback);
		nioService.stop(new CompletionCallback() {
			@Override
			public void onComplete() {
				callbackWaitAll.onComplete();
			}

			@Override
			public void onException(Exception exception) {
				callbackWaitAll.onException(exception);
			}
		});
		nioService.getNioEventloop().keepAlive(false);
	}
}
