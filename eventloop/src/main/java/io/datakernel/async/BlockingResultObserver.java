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
import java.util.concurrent.TimeoutException;

/**
 * Callback which allows to block running while result will be not receive.
 *
 * @param <T> type of result
 */
public class BlockingResultObserver<T> implements ResultCallback<T> {
	private final CountDownLatch latch = new CountDownLatch(1);
	private volatile T result;
	private volatile Exception e;

	/**
	 * Saves the result and unblocks running
	 */
	@Override
	public void onResult(T result) {
		this.result = result;
		latch.countDown();
	}

	/**
	 * Saves the exception and unblocks running
	 */
	@Override
	public void onException(Exception e) {
		this.e = e;
		latch.countDown();
	}

	/**
	 * Blocks running during timeout, if in this time took place receiving result, returns the result
	 * and throws exception if it has been. Else return false.
	 *
	 * @param timeout  the maximum time to wait
	 * @param timeUnit the time unit of the timeout argument
	 */
	public T getResult(long timeout, TimeUnit timeUnit) throws Exception {
		if (!latch.await(timeout, timeUnit))
			throw new TimeoutException("Result are not received after timeout");
		if (e != null)
			throw e;
		return result;
	}

	/**
	 * Blocks running until  take place receiving result. After unblocking  returns the result and throws
	 * exception if it has been.
	 */
	public T getResult() throws Exception {
		latch.await();
		if (e != null)
			throw e;
		return result;
	}

}
