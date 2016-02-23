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
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;

public final class AsyncOperationsTracker<T> {
	private final ResultCallback<List<T>> callback;

	private List<T> results = new ArrayList<>();
	private Exception exception;

	private int operations;
	private boolean shutDown;

	private final ResultCallback<List<T>> internalCallback = new ResultCallback<List<T>>() {
		@Override
		public void onResult(List<T> resultList) {
			--operations;
			results.addAll(resultList);
			returnResultsIfNeeded();
		}

		@Override
		public void onException(Exception e) {
			--operations;
			exception = e;
			returnResultsIfNeeded();
		}
	};

	public AsyncOperationsTracker(ResultCallback<List<T>> callback) {
		this.callback = callback;
	}

	public ResultCallback<List<T>> startOperation() {
		checkArgument(!shutDown);
		++operations;
		return internalCallback;
	}

	public void shutDown() {
		shutDown = true;
		returnResultsIfNeeded();
	}

	public void shutDownWithException(Exception e) {
		exception = e;
		shutDown = true;
		returnResultsIfNeeded();
	}

	public int getOperationsCount() {
		return operations;
	}

	private void returnResultsIfNeeded() {
		if (!shutDown || operations > 0)
			return;

		if (exception != null) {
			callback.onException(exception);
			return;
		}

		callback.onResult(results);
	}
}
