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

package io.datakernel.datastream;

import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;

/**
 * A wrapper class that delegates all calls to underlying {@link StreamSupplier}.
 * It exists for when one method of some supplier needs to be altered.
 */
public abstract class ForwardingStreamSupplier<T> implements StreamSupplier<T> {
	protected final StreamSupplier<T> supplier;

	public ForwardingStreamSupplier(StreamSupplier<T> supplier) {
		this.supplier = supplier;
	}

	@Override
	public Promise<Void> streamTo(@NotNull StreamConsumer<T> consumer) {
		return supplier.streamTo(consumer);
	}

	@Override
	public void updateDataAcceptor() {
		supplier.updateDataAcceptor();
	}

	@Override
	public Promise<Void> getEndOfStream() {
		return supplier.getEndOfStream();
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
		supplier.closeEx(e);
	}
}
