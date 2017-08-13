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

import java.util.concurrent.*;

public final class ResultCallbackFuture<T> extends ResultCallback<T> implements Future<T> {
	private final CountDownLatch latch = new CountDownLatch(1);
	private T result;
	private Exception exception;

	// region builders
	private ResultCallbackFuture() {
	}

	public static <T> ResultCallbackFuture<T> create() {
		return new ResultCallbackFuture<>();
	}

	// endregion

	@Override
	protected void onResult(T result) {
		this.result = result;
		latch.countDown();
	}

	@Override
	protected void onException(Exception exception) {
		this.exception = exception;
		latch.countDown();
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		// not implemented
		return false;
	}

	@Override
	public boolean isCancelled() {
		// not implemented
		return false;
	}

	@Override
	public boolean isDone() {
		return latch.getCount() == 0;
	}

	public boolean isComplete() {
		return isDone() && exception == null;
	}

	public boolean isException() {
		return isDone() && exception != null;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		latch.await();
		if (exception != null) {
			throw new ExecutionException(exception);
		}
		return result;
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		boolean computationCompleted = latch.await(timeout, unit);
		if (computationCompleted) {
			if (exception != null) {
				throw new ExecutionException(exception);
			} else {
				return result;
			}
		} else {
			throw new TimeoutException();
		}
	}

	@Override
	public String toString() {
		return "{" + (result != null ? result : (exception != null ? exception : "?")) + '}';
	}
}
