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
 * This callback is wrapper over other callback. It redirects callings fireException and sendResult to
 * it to other callback.
 */
public abstract class ForwardingCompletionCallback extends CompletionCallback {
	private final ExceptionCallback callback;

	/**
	 * Creates a new instance of ForwardingCompletionCallback with other callback
	 *
	 * @param callback callback for redirecting
	 */
	public ForwardingCompletionCallback(ExceptionCallback callback) {
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
