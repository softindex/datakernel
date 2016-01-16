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
import io.datakernel.eventloop.ScheduledRunnable;

import java.util.concurrent.TimeoutException;

/**
 * Represents a ResultCallback which has time to live. After timeout this callback can not be calling
 *
 * @param <T> type of received result
 */
public final class ResultCallbackWithTimeout<T> extends AbstractAsyncCancellable implements ResultCallback<T> {
	private final ResultCallback<T> callback;
	private final ScheduledRunnable timeouter;

	/**
	 * Creates a new instance of ResultCallbackWithTimeout
	 *
	 * @param eventloop     eventloop in which it will handle time
	 * @param callback      callback which will be called
	 * @param timeoutMillis time to live for this callback
	 */
	public ResultCallbackWithTimeout(Eventloop eventloop, final ResultCallback<T> callback, long timeoutMillis) {
		this.callback = callback;

		timeouter = eventloop.schedule(eventloop.currentTimeMillis() + timeoutMillis, new Runnable() {
			@Override
			public void run() {
				cancel();
				callback.onException(new TimeoutException());
			}
		});

		AsyncCallbacks.notifyOnCancel(callback, new CancelNotifier() {
			@Override
			public void onCancel() {
				cancel();
				timeouter.cancel();
			}
		});
	}

	@Override
	public void onResult(T result) {
		if (!timeouter.isCancelled() && !timeouter.isComplete()) {
			callback.onResult(result);
			timeouter.cancel();
		}
	}

	@Override
	public void onException(Exception exception) {
		if (!timeouter.isCancelled() && !timeouter.isComplete()) {
			callback.onException(exception);
			timeouter.cancel();
		}
	}
}
