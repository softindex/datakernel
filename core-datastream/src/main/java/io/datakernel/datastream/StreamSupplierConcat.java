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

import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

import static io.datakernel.eventloop.RunnableWithContext.wrapContext;

/**
 * Represents {@link StreamSupplier}, which created with iterator with {@link AbstractStreamSupplier}
 * which will stream to this
 *
 * @param <T> type of received data
 */
class StreamSupplierConcat<T> extends AbstractStreamSupplier<T> {
	private final Iterator<StreamSupplier<T>> iterator;
	@Nullable
	private StreamSupplier<T> supplier;
	@Nullable
	private InternalConsumer internalConsumer;

	StreamSupplierConcat(Iterator<StreamSupplier<T>> iterator) {
		this.iterator = iterator;
	}

	private class InternalConsumer extends AbstractStreamConsumer<T> {
		@Override
		protected Promise<Void> onEndOfStream() {
			eventloop.post(wrapContext(this, () -> {
				supplier = null;
				internalConsumer = null;
				if (isReceiverReady()) {
					onProduce(getCurrentDataAcceptor());
				}
			}));
			return StreamSupplierConcat.this.getConsumer().getAcknowledgement();
		}

		@Override
		protected void onError(Throwable e) {
			StreamSupplierConcat.this.close(e);
		}
	}

	@Override
	protected void onProduce(@NotNull StreamDataAcceptor<T> dataAcceptor) {
		if (supplier == null) {
			if (!iterator.hasNext()) {
				eventloop.post(wrapContext(this, this::sendEndOfStream));
				return;
			}
			supplier = iterator.next();
			internalConsumer = new InternalConsumer();
			supplier.streamTo(internalConsumer);
		}
		supplier.resume(dataAcceptor);
	}

	@Override
	protected void onSuspended() {
		if (supplier != null) {
			supplier.suspend();
		}
	}

	@Override
	protected void onError(Throwable e) {
		if (supplier != null) {
			assert internalConsumer != null;
			internalConsumer.close(e);
		} else {
			// TODO ?
		}
	}

	@Override
	protected void cleanup() {
		supplier = null;
	}

}
