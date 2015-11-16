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

public class ConcurrentServiceCallbacks {
	private ConcurrentServiceCallbacks() {
	}

	public static CountDownServiceCallback withCountDownLatch() {
		return new CountDownServiceCallback();
	}

	public static class CountDownServiceCallback extends ConcurrentServiceCallback {
		CountDownLatch countDownLatch = new CountDownLatch(1);

		@Override
		protected void doOnComplete() {
			countDownLatch.countDown();
		}

		@Override
		protected void doOnExeption(Exception exception) {
			countDownLatch.countDown();
		}

		public void await() throws InterruptedException {
			countDownLatch.await();
		}
	}
}
