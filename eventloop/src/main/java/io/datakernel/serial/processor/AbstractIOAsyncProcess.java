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

package io.datakernel.serial.processor;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.async.Stage;
import io.datakernel.exception.StacklessException;
import io.datakernel.serial.*;

import static io.datakernel.util.Recyclable.tryRecycle;

public abstract class AbstractIOAsyncProcess extends AbstractAsyncProcess {
	public static final StacklessException ASYNC_PROCESS_IS_COMPLETE = new StacklessException("AsyncProcess is complete");

	protected final <T> SerialSupplier<T> sanitize(SerialSupplier<T> supplier) {
		return new AbstractSerialSupplier<T>() {
			@Override
			protected Stage<T> doGet() {
				return sanitize(supplier.get());
			}

			@Override
			protected void onClosed(Throwable e) {
				supplier.close(e);
				AbstractIOAsyncProcess.this.close(e);
			}
		};
	}

	protected final <T> SerialConsumer<T> sanitize(SerialConsumer<T> consumer) {
		return new AbstractSerialConsumer<T>() {
			@Override
			protected Stage<Void> doAccept(@Nullable T item) {
				return sanitize(consumer.accept(item));
			}

			@Override
			protected void onClosed(Throwable e) {
				consumer.close(e);
				AbstractIOAsyncProcess.this.close(e);
			}
		};
	}

	protected final ByteBufsSupplier sanitize(ByteBufsSupplier supplier) {
		return new ByteBufsSupplier(supplier.bufs) {
			@Override
			public Stage<Void> needMoreData() {
				return sanitize(supplier.needMoreData());
			}

			@Override
			public Stage<Void> endOfStream() {
				return sanitize(supplier.endOfStream());
			}

			@Override
			public void close(Throwable e) {
				supplier.close(e);
				AbstractIOAsyncProcess.this.close(e);
			}
		};
	}

	protected final <T> Stage<T> sanitize(Stage<T> stage) {
		assert !isProcessComplete();
		return stage
				.thenComposeEx((value, e) -> {
					if (isProcessComplete()) {
						tryRecycle(value);
						return Stage.ofException(ASYNC_PROCESS_IS_COMPLETE);
					}
					if (e == null) {
						return Stage.of(value);
					} else {
						close(e);
						return Stage.ofException(e);
					}
				});
	}

}
