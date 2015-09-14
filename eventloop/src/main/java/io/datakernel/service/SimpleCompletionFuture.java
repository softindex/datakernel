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

public class SimpleCompletionFuture {
	private CountDownLatch latch = new CountDownLatch(1);
	private Exception exception;

	protected void doOnSuccess() {

	}

	public final void onSuccess() {
		doOnSuccess();
		latch.countDown();
	}

	protected void doOnError(Exception e) {

	}

	public final void onError(Exception e) {
		doOnError(e);
		exception = e;
		latch.countDown();
	}

	public final void await() throws Exception {
		latch.await();
		if (exception != null) throw exception;
	}
}
