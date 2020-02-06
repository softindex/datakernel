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

package io.datakernel.datastream;

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents {@link StreamSupplier}, which created with iterator with {@link AbstractStreamSupplier}
 * which will stream to this
 *
 * @param <T> type of received data
 */
class StreamSupplierConcat<T> implements StreamSupplier<T> {
	private final ChannelSupplier<StreamSupplier<T>> iterator;
	private boolean iteratorGet;
	@Nullable StreamDataAcceptor<T> dataAcceptor;
	@Nullable
	private StreamSupplier<T> supplier;
	private final SettablePromise<Void> endOfStream = new SettablePromise<>();

	StreamSupplierConcat(ChannelSupplier<StreamSupplier<T>> iterator) {
		this.iterator = iterator;
		next();
	}

	@Override
	public void supply(@Nullable StreamDataAcceptor<T> dataAcceptor) {
		if (endOfStream.isComplete()) return;
		if (this.dataAcceptor == dataAcceptor) return;
		this.dataAcceptor = dataAcceptor;
		if (supplier != null) {
			supplier.supply(dataAcceptor);
		}
	}

	private void next() {
		if (endOfStream.isComplete()) return;
		if (iteratorGet) return;
		iteratorGet = true;
		iterator.get()
				.whenResult(supplier -> {
					iteratorGet = false;
					this.supplier = supplier;
					if (supplier != null) {
						supplier.getEndOfStream()
								.async()
								.whenResult(this::next)
								.whenException(this::close);
						assert !endOfStream.isResult(); // should not happen
						if (endOfStream.isException()) {
							//noinspection ConstantConditions
							close(endOfStream.getException());
						} else {
							if (this.dataAcceptor != null) {
								supplier.supply(this.dataAcceptor);
							}
						}
					} else {
						endOfStream.set(null);
					}
					if (endOfStream.isException()) {
						iterator.close(endOfStream.getException());
					}
				})
				.whenException(this::close);
	}

	@Override
	public Promise<Void> getEndOfStream() {
		return endOfStream;
	}

	@Override
	public void close(@NotNull Throwable e) {
		endOfStream.trySetException(e);
		if (supplier != null) {
			supplier.close(e);
			supplier = null;
		}
		if (!iteratorGet) {
			iterator.close(e);
		}
	}

}
