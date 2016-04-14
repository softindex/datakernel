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
 * It is CompletionCallback which has time to live, in this time this callback will work as usual CompletionCallback,
 * after timeout, calling of it will throw TimeoutException.
 */
public final class CompletionCallbackWithTimeout extends CompletionCallback implements AsyncCancellable {
	public static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();
	private final CompletionCallback callback;
	private final ScheduledRunnable timeouter;

	/**
	 * Creates a new  CompletionCallbackWithTimeout
	 *
	 * @param eventloop     the eventloop in which it will monitoring time
	 * @param callback      the callback which will be called
	 * @param timeoutMillis time to live this CompletionCallbackWithTimeout
	 */
	public CompletionCallbackWithTimeout(Eventloop eventloop, final CompletionCallback callback, long timeoutMillis) {
		this.callback = callback;
		timeouter = eventloop.schedule(eventloop.currentTimeMillis() + timeoutMillis, new Runnable() {
			@Override
			public void run() {
				callback.fireException(TIMEOUT_EXCEPTION);
			}
		});
	}

	@Override
	protected void onComplete() {
		if (!timeouter.isCancelled() && !timeouter.isComplete()) {
			callback.complete();
			timeouter.cancel();
		}
	}

	@Override
	protected void onException(Exception exception) {
		if (!timeouter.isCancelled() && !timeouter.isComplete()) {
			callback.fireException(exception);
			timeouter.cancel();
		}
	}

	@Override
	public void cancel() {
		timeouter.cancel();
	}
}
