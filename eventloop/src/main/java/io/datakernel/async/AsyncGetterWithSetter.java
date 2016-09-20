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
public final class AsyncGetterWithSetter<T> implements AsyncGetter<T>, ResultCallback<T> {

	private final Eventloop eventloop;
	private T result;
	private Exception exception;
	private ResultCallback<T> callback;

	// region builders
	private AsyncGetterWithSetter(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	/**
	 * Initialize new instance of this class with event loop in which ResultCallback will be called
	 */
	public static <T> AsyncGetterWithSetter<T> create(Eventloop eventloop) {
		return new AsyncGetterWithSetter<>(eventloop);
	}
	// endregion

	private void fireResult() {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.onResult(AsyncGetterWithSetter.this.result);
			}
		});
	}

	private void fireException() {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.onException(AsyncGetterWithSetter.this.exception);
			}
		});
	}

	/**
	 * Handles result, saves it in this instance and if there is callback, posts calling its onResult() to
	 * Eventloop
	 *
	 * @param result received result
	 */
	@Override
	public void onResult(T result) {
		check(this.result == null && exception == null);
		this.result = checkNotNull(result);
		if (callback != null) {
			fireResult();
		}
	}

	/**
	 * Handles exception , saves it in this instance and if there is callback, posts calling its
	 * onException() to Eventloop
	 *
	 * @param exception exception that was throwing
	 */
	@Override
	public void onException(Exception exception) {
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
			fireResult();
		} else if (exception != null) {
			fireException();
		} else {
			this.callback = callback;
		}
	}
}
