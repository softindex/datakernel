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

import io.datakernel.eventloop.Eventloop;

import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * Instance of this class combines possibility of  getter and ResultCallback
 *
 * @param <T> type of result
 */
public final class AsyncGetterWithSetter<T> extends ResultCallback<T> implements AsyncGetter<T> {

	private final Eventloop eventloop;
	private T result;
	private Exception exception;
	private ResultCallback<T> callback;

	/**
	 * Initialize new instance of this class with event loop in which thread will be called
	 * ResultCallback
	 */
	public AsyncGetterWithSetter(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	private void sendResult() {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.sendResult(AsyncGetterWithSetter.this.result);
			}
		});
	}

	private void fireException() {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.fireException(AsyncGetterWithSetter.this.exception);
			}
		});
	}

	/**
	 * Handles result, saves it in this instance and if there is callback, posts calling its sendResult() to
	 * Eventloop
	 *
	 * @param result received result
	 */
	@Override
	protected void onResult(T result) {
		check(this.result == null && exception == null);
		this.result = checkNotNull(result);
		if (callback != null) {
			sendResult();
		}
	}

	/**
	 * Handles exception , saves it in this instance and if there is callback, posts calling its
	 * fireException() to Eventloop
	 *
	 * @param exception exception that was throwing
	 */
	@Override
	protected void onException(Exception exception) {
		check(this.result == null && exception == null);
		this.exception = exception;
		if (callback != null) {
			fireException();
		}
	}

	/**
	 * Gets results from this instance, if result or exception have been already received handles
	 * it, else saves callback from argument for further processing
	 */
	@Override
	public void get(ResultCallback<T> callback) {
		if (result != null) {
			sendResult();
		} else if (exception != null) {
			fireException();
		} else {
			this.callback = callback;
		}
	}
}
