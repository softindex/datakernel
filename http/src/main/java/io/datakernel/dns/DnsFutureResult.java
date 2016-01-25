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

package io.datakernel.dns;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

final class DnsFutureResult<V> {
	private volatile ConcurrentLinkedQueue<Runnable> listeners = new ConcurrentLinkedQueue<>();

	private volatile AtomicBoolean isSet = new AtomicBoolean(false);

	private volatile Throwable throwable;
	private volatile V value;

	private DnsFutureResult() {

	}

	private DnsFutureResult(V value) {
		this.value = value;
	}

	public static <V> DnsFutureResult<V> create() {
		return new DnsFutureResult<>();
	}

	public static <V> DnsFutureResult<V> immediateFuture(V value) {
		return new DnsFutureResult<>(value);
	}

	// Claimed to have already processed value when called
	public V get() throws ExecutionException {
		if (throwable != null) {
			throw new ExecutionException(throwable);
		}
		return value;
	}

	public synchronized void set(V value) {
		if (!isSet.get() && isSet.compareAndSet(false, true)) {
			this.value = value;
			notifyListeners();
		}
	}

	public synchronized void setException(Throwable throwable) {
		if (!isSet.get() && isSet.compareAndSet(false, true)) {
			this.throwable = throwable;
			notifyListeners();
		}
	}

	private void notifyListeners() {
		for (Runnable r : listeners) {
			r.run();
		}
		listeners = null;
	}

	public synchronized void addListener(Runnable runnable) {
		if (isSet.get()) {
			runnable.run();
		} else {
			listeners.add(runnable);
		}
	}
}
