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
 * Represents the IteratorCallback which has capabilities from  ForwardingCallback. It can be redirected to other callback.
 *
 * @param <T> type of elements in iterator
 */
public abstract class ForwardingIteratorCallback<T> extends IteratorCallback<T> {
	private final ExceptionCallback callback;

	/**
	 * Redirects all callings callbacks to callback from argument
	 */
	public ForwardingIteratorCallback(ExceptionCallback callback) {
		this.callback = callback;
	}

	/**
	 * Redirects the processing exception to other callback
	 *
	 * @param exception exception that was throwing
	 */
	@Override
	protected void onException(Exception exception) {
		callback.fireException(exception);
	}
}
