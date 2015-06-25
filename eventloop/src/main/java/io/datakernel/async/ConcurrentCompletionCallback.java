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

import io.datakernel.eventloop.NioEventloop;

/**
 * Represents a CompletionCallback which forwards result to another eventloop
 */
public final class ConcurrentCompletionCallback implements CompletionCallback {
	private final NioEventloop eventloop;
	private final CompletionCallback callback;

	/**
	 * Creates a new instance of ConcurrentResultCallback
	 *
	 * @param eventloop eventloop in which it will handle result
	 * @param callback  callback which will be handle result
	 */
	public ConcurrentCompletionCallback(NioEventloop eventloop, CompletionCallback callback) {
		this.eventloop = eventloop;
		this.callback = callback;
	}

	@Override
	public void onComplete() {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				callback.onComplete();
			}
		});
	}

	@Override
	public void onException(final Exception exception) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				callback.onException(exception);
			}
		});
	}
}
