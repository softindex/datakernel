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

package io.datakernel.stream;

import io.datakernel.async.AsyncIterator;
import io.datakernel.async.IteratorCallback;
import io.datakernel.eventloop.Eventloop;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Represents AsyncIterator which can call its callbacks with next element in eventloop's thread
 *
 * @param <T> type of element in iterator
 */
public class AsyncIteratorWithSetter<T> implements AsyncIterator<T> {
	private final Eventloop eventloop;

	private IteratorCallback<T> callback;
	private T next;
	private boolean end;
	private Exception exception;

	/**
	 * Creates a new AsyncIteratorWithSetter with eventloop from argument
	 */
	public AsyncIteratorWithSetter(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	/**
	 * Processes  the next element with eventloop
	 */
	@Override
	public void next(IteratorCallback<T> callback) {
		if (next != null) {
			sendNext(callback, next);
			this.next = null;
		} else if (end) {
			end(callback);
		} else if (exception != null) {
			fireException(callback, exception);
			this.exception = null;
		} else {
			this.callback = callback;
		}
	}

	private void sendNext(final IteratorCallback<T> callback, final T next) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.sendNext(next);
			}
		});
	}

	private void end(final IteratorCallback<T> callback) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.end();
			}
		});
	}

	private void fireException(final IteratorCallback<T> callback, final Exception exception) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.fireException(exception);
			}
		});
	}

	public void onNext(T item) {
		checkState(next == null && !end && exception == null);
		checkNotNull(item);
		if (callback != null) {
			sendNext(callback, item);
			callback = null;
		} else {
			this.next = item;
		}
	}

	public void onEnd() {
		checkState(next == null && !end && exception == null);
		if (callback != null) {
			end(callback);
			callback = null;
		} else {
			this.end = true;
		}
	}

	public void onException(Exception exception) {
		checkState(next == null && !end && this.exception == null);
		if (callback != null) {
			fireException(callback, exception);
			callback = null;
		} else {
			this.exception = exception;
		}
	}

	/**
	 * Returns new AsyncIteratorWithSetter with eventloop from argument
	 *
	 * @param <T> type of element from iterator
	 */
	public static <T> AsyncIteratorWithSetter<T> createAsyncIteratorWithSetter(Eventloop eventloop) {
		return new AsyncIteratorWithSetter<>(eventloop);
	}
}
