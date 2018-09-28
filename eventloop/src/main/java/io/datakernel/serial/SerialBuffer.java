/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;

import static io.datakernel.util.Recyclable.tryRecycle;
import static java.lang.Integer.numberOfLeadingZeros;

public final class SerialBuffer<T> implements SerialQueue<T>, Cancellable {
	private Exception exception;

	@SuppressWarnings("unchecked")
	private Object[] elements;
	private int tail;
	private int head;

	private final int bufferMinSize;
	private final int bufferMaxSize;

	@Nullable
	private SettableStage<Void> put;
	@Nullable
	private SettableStage<T> take;

	public SerialBuffer(int bufferSize) {
		this(0, bufferSize);
	}

	public SerialBuffer(int bufferMinSize, int bufferMaxSize) {
		this.bufferMinSize = bufferMinSize + 1;
		this.bufferMaxSize = bufferMaxSize;
		this.elements = new Object[32 - numberOfLeadingZeros(bufferMaxSize + 1)];
	}

	public boolean isSaturated() {
		return size() > bufferMaxSize;
	}

	public boolean willBeSaturated() {
		return size() >= bufferMaxSize;
	}

	public boolean isExhausted() {
		return size() < bufferMinSize;
	}

	public boolean willBeExhausted() {
		return size() <= bufferMinSize;
	}

	public boolean isPendingPut() {
		return put != null;
	}

	public boolean isPendingTake() {
		return take != null;
	}

	public int size() {
		return (tail - head) & (elements.length - 1);
	}

	public boolean isEmpty() {
		return tail == head;
	}

	@SuppressWarnings("unchecked")
	public void add(@Nullable T value) throws Exception {
		if (exception != null) throw exception;

		if (take != null) {
			assert isEmpty();
			SettableStage<T> take = this.take;
			this.take = null;
			take.set(value);
			if (exception != null) throw exception;
			return;
		}

		doAdd(value);
	}

	private void doAdd(@Nullable T value) {
		elements[tail] = value;
		tail = (tail + 1) & (elements.length - 1);
		if (tail == head) {
			doubleCapacity();
		}
	}

	private void doubleCapacity() {
		assert head == tail;
		int r = elements.length - head;
		Object[] newElements = new Object[elements.length << 1];
		System.arraycopy(elements, head, newElements, 0, r);
		System.arraycopy(elements, 0, newElements, r, head);
		elements = newElements;
		head = 0;
		tail = elements.length;
	}

	@Nullable
	@SuppressWarnings("unchecked")
	public T poll() throws Exception {
		if (exception != null) throw exception;

		if (put != null && willBeExhausted()) {
			T item = doPoll();
			SettableStage<Void> put = this.put;
			this.put = null;
			put.set(null);
			return item;
		}

		return !isEmpty() ? doPoll() : null;
	}

	private T doPoll() {
		assert head != tail;
		@SuppressWarnings("unchecked")
		T result = (T) elements[head];
		elements[head] = null;     // Must null out slot
		head = (head + 1) & (elements.length - 1);
		return result;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Stage<Void> put(@Nullable T value) {
		assert put == null;
		if (exception != null) return Stage.ofException(exception);

		if (take != null) {
			assert isEmpty();
			SettableStage<T> take = this.take;
			this.take = null;
			take.set(value);
			return Stage.complete();
		}

		doAdd(value);

		if (isSaturated()) {
			put = new SettableStage<>();
			return put;
		} else {
			return Stage.complete();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Stage<T> take() {
		assert take == null;
		if (exception != null) return Stage.ofException(exception);

		if (put != null && willBeExhausted()) {
			assert !isEmpty();
			T item = doPoll();
			SettableStage<Void> put = this.put;
			this.put = null;
			put.set(null);
			return Stage.of(item);
		}

		if (!isEmpty()) {
			return Stage.of(doPoll());
		}

		take = new SettableStage<>();
		return take;
	}

	@Override
	public void closeWithError(Throwable e) {
		if (exception != null) return;
		exception = e instanceof Exception ? (Exception) e : new RuntimeException(e);
		if (put != null) {
			put.setException(e);
			put = null;
		}
		if (take != null) {
			take.setException(e);
			take = null;
		}
		for (int i = head; i != tail; i = (i + 1) & (elements.length - 1)) {
			tryRecycle(elements[i]);
		}
		elements = null;
	}

	@Nullable
	public Throwable getException() {
		return exception;
	}
}
