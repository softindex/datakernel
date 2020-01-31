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

import java.util.Set;

public abstract class ForwardingStreamSupplier<T> implements StreamSupplier<T> {
	protected final StreamSupplier<T> supplier;

	public ForwardingStreamSupplier(StreamSupplier<T> supplier) {
		this.supplier = supplier;
	}

	@Override
	public void setConsumer(@NotNull StreamConsumer<T> consumer) {
		supplier.setConsumer(consumer);
	}

	@Override
	public void resume(@NotNull StreamDataAcceptor<T> dataAcceptor) {
		supplier.resume(dataAcceptor);
	}

	@Override
	public void suspend() {
		supplier.suspend();
	}

	@Override
	public Promise<Void> getEndOfStream() {
		return supplier.getEndOfStream();
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return supplier.getCapabilities();
	}

	@Override
	public void close(@NotNull Throwable e) {
		supplier.close(e);
	}
}
