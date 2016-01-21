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

package io.datakernel.util;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SettableFuture<V> implements ListenableFuture<V> {
	private List<Runnable> listeners = new CopyOnWriteArrayList<>();

	private final Lock lock = new ReentrantLock();
	private final CountDownLatch latch = new CountDownLatch(1);

	private Throwable throwable;
	private V value;

	private SettableFuture() {
	}

	private SettableFuture(V value) {
		this.value = value;
	}

	public static <V> SettableFuture<V> create() {
		return new SettableFuture<>();
	}

	public static <V> SettableFuture<V> immediateFuture(V value) {
		return new SettableFuture<>(value);
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		latch.await();
		if (throwable != null) {
			throw new ExecutionException(throwable);
		}
		return value;
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		boolean computationCompleted = latch.await(timeout, unit);
		if (computationCompleted) {
			if (throwable != null) {
				throw new ExecutionException(throwable);
			} else {
				return value;
			}
		} else {
			throw new TimeoutException();
		}
	}

	public void setException(Throwable throwable) {
		if (lock.tryLock()) {
			try {
				if (latch.getCount() != 0) {
					this.throwable = throwable;
					latch.countDown();
					notifyListeners();
				}
			} finally {
				lock.unlock();
			}
		}
	}

	public void set(V value) {
		if (lock.tryLock()) {
			try {
				if (latch.getCount() != 0) {
					this.value = value;
					latch.countDown();
					notifyListeners();
				}
			} finally {
				lock.unlock();
			}
		}
	}

	private void notifyListeners() {
		for (Runnable r : listeners) {
			r.run();
		}
	}

	@Override
	public void addListener(Runnable runnable) {
		if (value != null || throwable != null) {
			runnable.run();
		} else {
			listeners.add(runnable);
		}
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		throw new NotImplementedException();
	}

	@Override
	public boolean isCancelled() {
		throw new NotImplementedException();
	}

	@Override
	public boolean isDone() {
		throw new NotImplementedException();
	}
}
