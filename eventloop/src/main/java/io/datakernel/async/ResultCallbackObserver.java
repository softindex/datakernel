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

/**
 * In case of a call onResult of this callback it stores received result, in case of a call
 * onException it  stores its exception. This callback can be used only once.
 * You should override method onResultOrException which handles received data.
 *
 * @param <T> type of received result
 */
public class ResultCallbackObserver<T> implements ResultCallback<T> {
	private boolean ready;
	private T result;
	private Exception exception;

	/**
	 * Sets its result, flag complete and calls onResultOrException()
	 *
	 * @param result received result
	 */
	@Override
	public void onResult(T result) {
		assert !ready;
		ready = true;
		this.result = result;
		onResultOrException();
	}

	/**
	 * Sets its exception, flag complete and calls onResultOrException()
	 *
	 * @param exception received exception
	 */
	@Override
	public void onException(Exception exception) {
		assert !ready && exception != null;
		ready = true;
		this.exception = exception;
		onResultOrException();
	}

	public final T getResult() {
		return result;
	}

	public final Exception getException() {
		return exception;
	}

	public final boolean isResultOrException() {
		return result != null || exception != null;
	}

	/**
	 * Returns result if it was received or throws its exception
	 *
	 * @throws Exception
	 */
	public final T getResultOrException() throws Exception {
		if (!ready)
			throw new IllegalStateException("Result not ready");
		if (exception != null)
			throw exception;
		return result;
	}

	/**
	 * Handles receiving data. This method is called after storing
	 * received result or exception
	 */
	protected void onResultOrException() {
	}

}
