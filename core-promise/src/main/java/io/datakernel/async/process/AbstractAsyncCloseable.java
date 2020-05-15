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

package io.datakernel.async.process;

import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.common.Recyclable.tryRecycle;

public abstract class AbstractAsyncCloseable implements AsyncCloseable {
	@Nullable
	private AsyncCloseable closeable;

	private Throwable exception;

	public Throwable getException() {
		return exception;
	}

	public void setCloseable(@Nullable AsyncCloseable closeable) {
		this.closeable = closeable;
	}

	protected void onClosed(@NotNull Throwable e) {
	}

	@Override
	public final void closeEx(@NotNull Throwable e) {
		if (isClosed()) return;
		exception = e;
		onClosed(e);
		if (closeable != null) {
			closeable.closeEx(e);
		}
	}

	public final boolean isClosed() {
		return exception != null;
	}

	@NotNull
	public final <T> Promise<T> sanitize(Promise<T> promise) {
		return promise.async()
				.thenEx(this::sanitize);
	}

	@NotNull
	public final <T> Promise<T> sanitize(T value, @Nullable Throwable e) {
		if (exception != null) {
			tryRecycle(value);
			if (value instanceof AsyncCloseable) {
				((AsyncCloseable) value).closeEx(exception);
			}
			return Promise.ofException(exception);
		}
		if (e == null) {
			return Promise.of(value);
		} else {
			closeEx(e);
			return Promise.ofException(e);
		}
	}

}
