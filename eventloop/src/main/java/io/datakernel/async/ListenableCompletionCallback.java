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

import java.util.ArrayList;
import java.util.Iterator;

/**
 * This callback contains collection of listeners-CompletionCallback, on calling onComplete/onException this callback
 * it will be call listeners methods too. Each listener can react only on one action, than it will be removed from
 * this ListenableCompletionCallback.
 */
public class ListenableCompletionCallback extends AbstractAsyncCancellable implements CompletionCallback {
	private ArrayList<CompletionCallback> callbackList = new ArrayList<>();
	private int cancelled = 0;
	private int cancellable = 0;
	private boolean completed = false;
	private Exception exception;

	public void addCallback(CompletionCallback callback) {
		if (callback instanceof AsyncCancellableStatus) {
			AsyncCancellableStatus cancellableCallback = (AsyncCancellableStatus) callback;
			boolean isCancelled = cancellableCallback.isCancelled();

			if (completed && !isCancelled) {
				callback.onComplete();
				return;
			}

			if (exception != null && !isCancelled) {
				callback.onException(exception);
				return;
			}

			++cancellable;

			if (isCancelled) {
				++cancelled;
			}

			cancellableCallback.notifyOnCancel(new CancelNotifier() {
				@Override
				public void onCancel() {
					++cancelled;

					if (cancelled == cancellable && cancellable == callbackList.size()) {
						cancel();
					}
				}
			});
		} else {
			if (completed) {
				callback.onComplete();
				return;
			}

			if (exception != null) {
				callback.onException(exception);
				return;
			}
		}

		callbackList.add(callback);
	}

	@Override
	public void onComplete() {
		this.completed = true;

		Iterator<CompletionCallback> it = callbackList.iterator();

		while (it.hasNext()) {
			CompletionCallback callback = it.next();

			if (callback instanceof AsyncCancellableStatus) {
				if (((AsyncCancellableStatus) callback).isCancelled()) {
					it.remove();
					continue;
				}
			}

			callback.onComplete();
			it.remove();
		}
	}

	@Override
	public void onException(Exception exception) {
		this.exception = exception;

		Iterator<CompletionCallback> it = callbackList.iterator();

		while (it.hasNext()) {
			CompletionCallback callback = it.next();

			if (callback instanceof AsyncCancellableStatus) {
				if (((AsyncCancellableStatus) callback).isCancelled()) {
					it.remove();
					continue;
				}
			}

			callback.onException(exception);
			it.remove();
		}
	}
}
