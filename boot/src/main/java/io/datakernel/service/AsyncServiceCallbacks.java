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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public final class AsyncServiceCallbacks {
	private AsyncServiceCallbacks() {
	}

	public static BlockingServiceCallback withCountDownLatch() {
		return new BlockingServiceCallback();
	}

	public static class BlockingServiceCallback implements AsyncServiceCallback {
		private final CountDownLatch countDownLatch = new CountDownLatch(1);
		private Exception exception;

		@Override
		public void onComplete() {
			countDownLatch.countDown();
		}

		@Override
		public void onException(Exception exception) {
			this.exception = exception;
			countDownLatch.countDown();
		}

		public void await() throws Exception {
			countDownLatch.await();
			if (exception != null) {
				throw new ExecutionException(exception);
			}
		}
	}
}
