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

public final class SerialZeroBuffer<T> implements SerialQueue<T>, Cancellable {
	private Exception exception;

	@Nullable
	private T value;

	private SettableStage<Void> put;
	private SettableStage<T> take;

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
	@SuppressWarnings("unchecked")
	public Stage<Void> put(@Nullable T value) {
		assert put == null;
		if (exception == null) {
			if (take != null) {
				SettableStage<T> take = this.take;
				this.take = null;
				take.set(value);
				return Stage.complete();
			}

			this.value = value;
			this.put = new SettableStage<>();
			return put;
		} else {
			tryRecycle(value);
			return Stage.ofException(exception);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stage<T> take() {
		assert take == null;
		if (exception == null) {
			if (put != null) {
				T value = this.value;
				SettableStage<Void> put = this.put;
				this.value = null;
				this.put = null;
				put.set(null);
				return Stage.of(value);
			}

			this.take = new SettableStage<>();
			return take;
		} else {
			return Stage.ofException(exception);
		}
	}

	@SuppressWarnings("unchecked")
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
		tryRecycle(value);
		value = null;
	}

	@Nullable
	public Throwable getException() {
		return exception;
	}
}
