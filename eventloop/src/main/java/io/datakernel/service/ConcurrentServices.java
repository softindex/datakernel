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
import io.datakernel.eventloop.NioService;

import java.util.Arrays;
import java.util.List;

import static io.datakernel.async.AsyncCallbacks.completionCallbackOfFuture;

public final class ConcurrentServices {
	private ConcurrentServices() {
	}

	/**
	 * Returns new  {@link ConcurrentService} which after each method returns ListenableFuture which has
	 * its value true
	 */
	public static ConcurrentService immediateService() {
		return new ConcurrentService() {
			@Override
			public ListenableFuture<?> startFuture() {
				return Futures.immediateFuture(true);
			}

			@Override
			public ListenableFuture<?> stopFuture() {
				return Futures.immediateFuture(true);
			}
		};
	}

	/**
	 * Returns {@link ConcurrentService} which after starting returns immediateFailedFuture and after
	 * stopping returns immediateFuture with its value true
	 */
	public static ConcurrentService immediateFailedService() {
		return new ConcurrentService() {
			@Override
			public ListenableFuture<?> startFuture() {
				return Futures.immediateFailedFuture(new Exception());
			}

			@Override
			public ListenableFuture<?> stopFuture() {
				return Futures.immediateFuture(true);
			}
		};
	}

	/**
	 * Starts and stops services in parallel way
	 *
	 * @param services list of services which will be processed
	 */
	public static ConcurrentService parallelService(ConcurrentService... services) {
		return new ParallelService(Arrays.asList(services));
	}

	/**
	 * Starts and stops services in parallel way
	 *
	 * @param services list of services which will be processed
	 */
	public static ConcurrentService parallelService(List<? extends ConcurrentService> services) {
		return new ParallelService(services);
	}

	/**
	 * Starts services sequentially, from first one to last one.
	 * Stops services sequentially in reverse order: from last one to first one.
	 *
	 * @param services list of services which will be processed
	 */
	public static ConcurrentService sequentialService(ConcurrentService... services) {
		return new SequentialService(Arrays.asList(services));
	}

	/**
	 * Starts services sequentially, from first one to last one.
	 * Stops services sequentially in reverse order: from last one to first one.
	 *
	 * @param services list of services which will be processed
	 */
	public static ConcurrentService sequentialService(List<? extends ConcurrentService> services) {
		return new SequentialService(services);
	}

	public static ListenableFuture<Void> startFuture(final NioService nioService) {
		final SettableFuture<Void> future = SettableFuture.create();
		nioService.getNioEventloop().postConcurrently(new Runnable() {
			@Override
			public void run() {
				nioService.start(completionCallbackOfFuture(future));
			}
		});
		return future;
	}

	public static ListenableFuture<Void> stopFuture(final NioService nioService) {
		final SettableFuture<Void> future = SettableFuture.create();
		nioService.getNioEventloop().postConcurrently(new Runnable() {
			@Override
			public void run() {
				nioService.stop(completionCallbackOfFuture(future));
			}
		});
		return future;
	}

	public static ConcurrentService concurrentServiceOfNioService(final NioService nioService) {
		return new ConcurrentService() {
			@Override
			public ListenableFuture<?> startFuture() {
				return ConcurrentServices.startFuture(nioService);
			}

			@Override
			public ListenableFuture<?> stopFuture() {
				return ConcurrentServices.stopFuture(nioService);
			}
		};
	}

}
