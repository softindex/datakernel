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

import com.google.common.collect.Queues;

import java.util.Deque;
import java.util.List;

public class BroadcastingResultCallback<T> extends AbstractAsyncCancellable implements ResultCallback<T> {
	private final Deque<ResultCallback<T>> listeners;

	public BroadcastingResultCallback(List<ResultCallback<T>> listeners) {
		this.listeners = Queues.newArrayDeque();

		for (final ResultCallback<T> listener : listeners) {
			if (listener instanceof AsyncCancellableStatus) {
				AsyncCancellableStatus cancellableListener = (AsyncCancellableStatus) listener;

				if (cancellableListener.isCancelled()) {
					continue;
				}

				this.listeners.add(listener);

				cancellableListener.notifyOnCancel(new CancelNotifier() {
					@Override
					public void onCancel() {
						BroadcastingResultCallback<T> self = BroadcastingResultCallback.this;
						self.listeners.remove(listener);
						cancelItselfIfNoListeners();
					}
				});
			} else {
				this.listeners.add(listener);
			}
		}

		cancelItselfIfNoListeners();
	}

	private void cancelItselfIfNoListeners() {
		if (this.listeners.isEmpty())
			cancel();
	}

	@Override
	public void onResult(T result) {
		for (ResultCallback<T> listener : listeners) {
			listener.onResult(result);
		}

		listeners.clear();
	}

	@Override
	public void onException(Exception exception) {
		for (ResultCallback<T> listener : listeners) {
			listener.onException(exception);
		}

		listeners.clear();
	}
}
