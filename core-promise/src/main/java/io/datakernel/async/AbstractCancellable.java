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

package io.datakernel.async;

import org.jetbrains.annotations.Nullable;

public abstract class AbstractCancellable implements Cancellable {
	@Nullable
	private Cancellable cancellable;

	@Nullable
	private Throwable exception;

	@Nullable
	public Throwable getException() {
		return exception;
	}

	public void setCancellable(@Nullable Cancellable cancellable) {
		this.cancellable = cancellable;
	}

	protected void onClosed(Throwable e) {
	}

	@Override
	public final void close(Throwable e) {
		if (isClosed()) return;
		exception = e;
		onClosed(e);
		if (cancellable != null) {
			cancellable.close(e);
		}
	}

	public final boolean isClosed() {
		return exception != null;
	}
}
