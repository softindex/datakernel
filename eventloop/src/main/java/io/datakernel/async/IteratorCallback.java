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
 * Represents the callback for iterator, which can process each occurred action with iterator
 *
 * @param <T>
 */
public abstract class IteratorCallback<T> extends ExceptionCallback {
	public final void sendNext(T item) {
		onNext(item);
	}

	/**
	 * Calls after calling next() from iterator, processed received value
	 *
	 * @param item received value from iterator
	 */
	protected abstract void onNext(T item);

	/**
	 * Calls after ending elements in iterator
	 */
	public final void end() {
		CallbackRegistry.complete(this);
		onEnd();
	}

	protected abstract void onEnd();
}
