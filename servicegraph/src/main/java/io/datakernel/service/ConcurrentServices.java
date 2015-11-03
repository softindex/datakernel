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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.SimpleCompletionFuture;
import io.datakernel.eventloop.NioService;

import java.util.Arrays;
import java.util.List;


public class ConcurrentServices {
	private ConcurrentServices() {
	}

	public static ConcurrentService immediateService() {
		return new ConcurrentService() {
			@Override
			public void startFuture(SimpleCompletionFuture callback) {
				callback.onSuccess();
			}

			@Override
			public void stopFuture(SimpleCompletionFuture callback) {
				callback.onSuccess();
			}
		};
	}

	public static ConcurrentService immediateFailedService(final Exception e) {
		return new ConcurrentService() {
			@Override
			public void startFuture(SimpleCompletionFuture callback) {
				callback.onError(e);
			}

			@Override
			public void stopFuture(SimpleCompletionFuture callback) {
				callback.onSuccess();
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

	public static void startFuture(final NioService nioService, final SimpleCompletionFuture callback) {
		nioService.getNioEventloop().postConcurrently(new Runnable() {
			@Override
			public void run() {
				nioService.start(completionCallbackOfServiceCallback(callback));
			}
		});
	}

	public static void stopFuture(final NioService nioService, final SimpleCompletionFuture callback) {
		nioService.getNioEventloop().postConcurrently(new Runnable() {
			@Override
			public void run() {
				nioService.stop(completionCallbackOfServiceCallback(callback));
			}
		});
	}

	public static ConcurrentService concurrentServiceOfNioServiceCallback(final NioService nioService, final SimpleCompletionFuture callback) {
		return new ConcurrentService() {
			@Override
			public void startFuture(SimpleCompletionFuture callback) {
				ConcurrentServices.startFuture(nioService, callback);
			}

			@Override
			public void stopFuture(SimpleCompletionFuture callback) {
				ConcurrentServices.stopFuture(nioService, callback);
			}
		};
	}

	public static CompletionCallback completionCallbackOfServiceCallback(SimpleCompletionFuture callback) {
		return new ComletionCallbackOfService(callback);
	}

	private static final class ComletionCallbackOfService implements CompletionCallback {
		private final SimpleCompletionFuture simpleCompletionFuture;

		private ComletionCallbackOfService(SimpleCompletionFuture simpleCompletionFuture) {this.simpleCompletionFuture = simpleCompletionFuture;}

		@Override
		public void onComplete() {
			simpleCompletionFuture.onSuccess();
		}

		@Override
		public void onException(Exception exception) {
			simpleCompletionFuture.onError(exception);
		}
	}
}
