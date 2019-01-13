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

package io.datakernel.csp.queue;

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.util.Recyclable.tryRecycle;

public final class ChannelZeroBuffer<T> implements ChannelQueue<T> {
	private Exception exception;

	@Nullable
	private T value;

	@Nullable
	private SettablePromise<Void> put;
	@Nullable
	private SettablePromise<T> take;

	public boolean isWaiting() {
		return take != null || put != null;
	}

	public boolean isWaitingPut() {
		return put != null;
	}

	public boolean isWaitingTake() {
		return take != null;
	}

	@Override
	public Promise<Void> put(@Nullable T value) {
		assert put == null;
		if (exception == null) {
			if (take != null) {
				SettablePromise<T> take = this.take;
				this.take = null;
				take.set(value);
				return Promise.complete();
			}

			this.value = value;
			this.put = new SettablePromise<>();
			return put;
		} else {
			tryRecycle(value);
			return Promise.ofException(exception);
		}
	}

	@Override
	public Promise<T> take() {
		assert take == null;
		if (exception == null) {
			if (put != null) {
				T value = this.value;
				SettablePromise<Void> put = this.put;
				this.value = null;
				this.put = null;
				put.set(null);
				return Promise.of(value);
			}

			this.take = new SettablePromise<>();
			return take;
		} else {
			return Promise.ofException(exception);
		}
	}

	@Override
	public void close(Throwable e) {
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
		tryRecycle(value);
		value = null;
	}

	@Nullable
	public Throwable getException() {
		return exception;
	}
}
