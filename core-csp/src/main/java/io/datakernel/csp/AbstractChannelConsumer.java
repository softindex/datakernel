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

package io.datakernel.csp;

import io.datakernel.async.AbstractCancellable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.util.Recyclable.tryRecycle;

public abstract class AbstractChannelConsumer<T> extends AbstractCancellable implements ChannelConsumer<T> {
	// region creators
	protected AbstractChannelConsumer() {
		setCancellable(null);
	}

	protected AbstractChannelConsumer(@Nullable Cancellable cancellable) {
		setCancellable(cancellable);
	}
	// endregion

	protected abstract Promise<Void> doAccept(@Nullable T value);

	@NotNull
	@Override
	public Promise<Void> accept(@Nullable T value) {
		if (isClosed()) {
			tryRecycle(value);
			return Promise.ofException(getException());
		}
		return doAccept(value);
	}
}
