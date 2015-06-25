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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;

/**
 * {@link ResultCallback} which contain list of callbacks which will be called in turn. The instance of this
 * class can respond to only one action onException or onResult
 *
 * @param <T> type of result
 */
public class AsyncCallbacksProxy<T> implements ResultCallback<T> {
	private static final Logger logger = LoggerFactory.getLogger(AsyncCallbacksProxy.class);

	/**
	 * List of callbacks for calling
	 */
	private final List<ResultCallback<T>> callbacks = newArrayList();

	private boolean alreadyExecuted = false;

	public AsyncCallbacksProxy() {
	}

	/**
	 * Creates a new instance of AsyncCallbacksProxy with ResultCallback
	 */
	public AsyncCallbacksProxy(ResultCallback<T> callback) {
		checkNotNull(callback);
		callbacks.add(callback);
	}

	/**
	 * Adds the ResultCallback to AsyncCallbacksProxy, if it has not be executed yet
	 *
	 * @param callback callback for adding
	 */
	synchronized public void subscribe(ResultCallback<T> callback) {
		checkState(!alreadyExecuted);
		checkNotNull(callback);
		callbacks.add(callback);
	}

	/**
	 * Calls all methods onException from callbacks. Removes all callbacks.
	 *
	 * @param exception exception that was throwing
	 */
	@Override
	synchronized public void onException(Exception exception) {
		checkState(!alreadyExecuted);
		for (ResultCallback<T> callback : callbacks) {
			try {
				callback.onException(exception);
			} catch (Exception e) {
				logger.error("Error while notifying callback on exception", e);
			}
		}
		callbacks.clear();
		alreadyExecuted = true;
	}

	/**
	 * Calls all methods onResult from callbacks. Removes all callbacks.
	 *
	 * @param result received result
	 */
	@Override
	synchronized public void onResult(T result) {
		checkState(!alreadyExecuted);
		for (ResultCallback<T> callback : callbacks) {
			try {
				callback.onResult(result);
			} catch (Exception e) {
				logger.error("Error while notifying callback on result", e);
			}
		}
		callbacks.clear();
		alreadyExecuted = true;
	}
}