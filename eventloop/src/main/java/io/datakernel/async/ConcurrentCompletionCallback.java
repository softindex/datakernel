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

public final class ConcurrentCompletionCallback extends CompletionCallback {
	private final CompletionCallback callback;
	private final Eventloop eventloop;

	private ConcurrentCompletionCallback(Eventloop eventloop, CompletionCallback callback) {
		this.callback = callback;
		this.eventloop = eventloop;
	}

	public static ConcurrentCompletionCallback create(Eventloop eventloop, CompletionCallback callback) {
		return new ConcurrentCompletionCallback(eventloop, callback);
	}

	@Override
	public void onComplete() {
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				callback.setComplete();
			}
		});
	}

	@Override
	public void onException(final Exception exception) {
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				callback.setException(exception);
			}
		});
	}
}
