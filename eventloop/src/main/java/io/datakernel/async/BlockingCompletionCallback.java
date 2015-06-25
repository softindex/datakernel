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
import java.util.concurrent.TimeUnit;

/**
 * Blocks running until take place predetermined number of its calling
 */
public class BlockingCompletionCallback implements CompletionCallback {
	private final CountDownLatch latch;
	private volatile Exception e;

	/**
	 * Create a new instance of BlockingCompletionCallback with blocking until take place the one
	 * calling
	 */
	public BlockingCompletionCallback() {
		this(1);
	}

	/**
	 * Creates a new instance of BlockingCompletionCallback with blocking until take place count
	 * calling
	 *
	 * @param count number of expected calling
	 */
	public BlockingCompletionCallback(int count) {
		this.latch = new CountDownLatch(count);
	}

	/**
	 * Increments number of callings
	 */
	@Override
	public void onComplete() {
		latch.countDown();
	}

	/**
	 * Increments number of callings. Update its exception
	 */
	@Override
	public void onException(Exception e) {
		this.e = e;
		latch.countDown();
	}

	/**
	 * Blocks running during timeout, if in this time took place predetermined number of calling,
	 * return true and throws exception if it has been. Else return false
	 *
	 * @param timeout  the maximum time to wait
	 * @param timeUnit the time unit of the timeout argument
	 */
	public boolean await(long timeout, TimeUnit timeUnit) throws Exception {
		if (!latch.await(timeout, timeUnit))
			return false;
		if (e != null)
			throw e;
		return true;
	}

	/**
	 * Blocks running until  take place predetermined number of its calling. After unblocking throws
	 * exception if it has been.
	 */
	public void await() throws Exception {
		latch.await();
		if (e != null)
			throw e;
	}
}
