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

package io.datakernel.async;

import java.util.concurrent.CountDownLatch;

public class SimpleResultFuture<T> {
	private CountDownLatch latch = new CountDownLatch(1);
	private T result;
	private Exception exception;

	protected void doOnResult(T item) {
	}

	public final void onResult(T item) {
		result = item;
		doOnResult(item);
		latch.countDown();
	}

	protected void doOnError(Exception e) {

	}

	public final void onError(Exception e) {
		doOnError(e);
		exception = e;
		latch.countDown();
	}

	public final T get() throws Exception {
		latch.await();
		if (exception != null) {
			throw exception;
		}
		return result;
	}
}
