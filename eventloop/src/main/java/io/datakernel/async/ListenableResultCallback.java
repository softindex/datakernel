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

import java.util.ArrayList;
import java.util.List;

/**
 * This callback contains collection of listeners-ResultCallbacks, on calling sendResult/fireException this callback
 * it will be call listeners methods too.
 *
 * @param <T> type of result
 */
public final class ListenableResultCallback<T> extends ResultCallback<T> {
	private List<ResultCallback<T>> listeners = new ArrayList<>();
	private T result;
	private Exception exception;

	/**
	 * Adds new listener and if this callback has result or exception it will call listeners methods
	 * for handle ir.
	 *
	 * @param listener new ResultCallback which will listen this callback
	 */
	public void addListener(ResultCallback<T> listener) {
		if (result != null) {
			listener.sendResult(result);
			return;
		}

		if (exception != null) {
			listener.fireException(exception);
			return;
		}

		listeners.add(listener);
	}

	/**
	 * Sets result to this callback and calls sendResult() from all listeners.
	 *
	 * @param result result of async operation
	 */
	@Override
	protected void onResult(T result) {
		this.result = result;

		for (ResultCallback<T> listener : listeners) {
			listener.sendResult(result);
		}

		listeners.clear();
	}

	/**
	 * Sets exception to this callback and calls fireException() from all listeners.
	 *
	 * @param exception exception of async operation
	 */
	@Override
	protected void onException(Exception exception) {
		this.exception = exception;

		for (ResultCallback<T> listener : listeners) {
			listener.fireException(exception);
		}

		listeners.clear();
	}
}

