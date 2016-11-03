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

public final class ConcurrentResultCallback<T> extends ResultCallback<T> {
	private final ResultCallback<T> callback;
	private final Eventloop eventloop;

	private ConcurrentResultCallback(ResultCallback<T> callback, Eventloop eventloop) {
		this.eventloop = eventloop;
		this.callback = callback;
	}

	public static <T> ConcurrentResultCallback<T> create(ResultCallback<T> callback, Eventloop eventloop) {
		return new ConcurrentResultCallback<>(callback, eventloop);
	}

	@Override
	public void onResult(final T result) {
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				callback.setResult(result);
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
