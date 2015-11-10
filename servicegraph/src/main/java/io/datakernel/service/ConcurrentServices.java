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

import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioServer;
import io.datakernel.eventloop.NioService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

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

	// TODO (vsavchuk) delete
	public static ConcurrentService ofNioService(final NioService nioService) {
		return new ConcurrentService() {
			@Override
			public void startFuture(final ConcurrentServiceCallback callback) {
				nioService.getNioEventloop().postConcurrently(new Runnable() {
					@Override
					public void run() {
						nioService.start(callback);
						callback.onComplete();
					}
				});
			}

			@Override
			public void stopFuture(final ConcurrentServiceCallback callback) {
				nioService.getNioEventloop().postConcurrently(new Runnable() {
					@Override
					public void run() {
						nioService.stop(callback);
						callback.onComplete();
					}
				});
			}
		};
	}

	// TODO (vsavchuk) delete
	public static ConcurrentService ofNioServer(final NioServer nioServer) {
		return new ConcurrentService() {
			@Override
			public void startFuture(final ConcurrentServiceCallback callback) {
				nioServer.getNioEventloop().postConcurrently(new Runnable() {
					@Override
					public void run() {
						try {
							nioServer.listen();
							callback.onComplete();
						} catch (IOException e) {
							callback.onException(e);
						}
					}
				});
			}

			@Override
			public void stopFuture(final ConcurrentServiceCallback callback) {
				nioServer.getNioEventloop().postConcurrently(new Runnable() {
					@Override
					public void run() {
						nioServer.close();
						callback.onComplete();
					}
				});
			}
		};
	}

	// TODO (vsavchuk) delete
	public static ConcurrentService ofNioEventloop(final NioEventloop nioEventloop) {
		return new ConcurrentService() {
			@Override
			public void startFuture(final ConcurrentServiceCallback callback) {
				// TODO (vsavchuk) see ServiceGraphFactory
				Executors.defaultThreadFactory().newThread(new Runnable() {
					@Override
					public void run() {
						nioEventloop.keepAlive(true);
						callback.onComplete();
						nioEventloop.run();
					}
				}).start();
			}

			@Override
			public void stopFuture(final ConcurrentServiceCallback callback) {
				nioEventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						callback.onComplete();
						nioEventloop.keepAlive(false);
					}
				});
			}
		};
	}
}
