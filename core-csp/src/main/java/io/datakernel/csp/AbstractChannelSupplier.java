/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import io.datakernel.async.process.AbstractAsyncCloseable;
import io.datakernel.async.process.AsyncCloseable;
import io.datakernel.common.Check;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.common.Preconditions.checkState;

public abstract class AbstractChannelSupplier<T> extends AbstractAsyncCloseable implements ChannelSupplier<T> {
	protected static final Boolean CHECK = Check.isEnabled(AbstractChannelSupplier.class);

	// region creators
	protected AbstractChannelSupplier() {
		setCloseable(null);
	}

	protected AbstractChannelSupplier(@Nullable AsyncCloseable closeable) {
		setCloseable(closeable);
	}
	// endregion

	protected abstract Promise<T> doGet();

	@NotNull
	@Override
	public final Promise<T> get() {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		return isClosed() ? Promise.ofException(getException()) : doGet();
	}
}
