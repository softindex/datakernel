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
 * This callback is wrapper over other callback. It redirects callings to it to other callback.
 */
public abstract class ForwardingCallback implements ExceptionCallback, AsyncCancellableStatus {
	private final ExceptionCallback callback;

	/**
	 * Creates a new instance of ForwardingCallback
	 *
	 * @param callback callback for redirecting
	 */
	public ForwardingCallback(ExceptionCallback callback) {
		this.callback = callback;
	}

	/**
	 * Redirects the processing exception to other callback
	 *
	 * @param exception exception that was throwing
	 */
	@Override
	public void onException(Exception exception) {
		callback.onException(exception);
	}

	/**
	 * Checks if other callback is cancelled
	 *
	 * @return true if it cancelled, false else
	 */
	@SuppressWarnings("SimplifiableConditionalExpression")
	@Override
	public final boolean isCancelled() {
		return AsyncCallbacks.isCancelled(callback);
	}

	/**
	 * If callback was cancelled calls onCancel of CancelNotifier
	 */
	@Override
	public final void notifyOnCancel(CancelNotifier cancelNotifier) {
		AsyncCallbacks.notifyOnCancel(callback, cancelNotifier);
	}
}
