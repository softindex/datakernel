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

public final class WaitAllHandler {
	private final int minCompleted;
	private final int totalCount;
	private final CompletionCallback callback;

	private int completed = 0;
	private int exceptions = 0;
	private Exception lastException;

	private WaitAllHandler(int minCompleted, int totalCount, CompletionCallback callback) {
		this.minCompleted = minCompleted;
		this.totalCount = totalCount;
		this.callback = callback;
	}

	public static WaitAllHandler create(int count, CompletionCallback callback) {
		return new WaitAllHandler(count, count, callback);
	}

	public static WaitAllHandler create(int minCompleted, int totalCount, CompletionCallback callback) {
		return new WaitAllHandler(minCompleted, totalCount, callback);
	}

	public CompletionCallback getCallback() {
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				++completed;
				completeResult();
			}

			@Override
			protected void onException(Exception exception) {
				++exceptions;
				lastException = exception;
				completeResult();
			}
		};
	}

	private void completeResult() {
		if ((exceptions + completed) == totalCount) {
			if (completed >= minCompleted) {
				callback.setComplete();
			} else {
				callback.setException(lastException);
			}
		}
	}
}
