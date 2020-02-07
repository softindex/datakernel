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
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.common.Preconditions.checkArgument;

/**
 * Provides you apply function before sending data to the destination. It is a {@link StreamConsumerSwitcher}
 * which receives specified type and streams set of function's result  to the destination .
 */
public final class StreamConsumerSwitcher<T> implements StreamConsumer<T> {
	private @Nullable StreamSupplier<T> supplier;
	private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

	private @Nullable StreamConsumer<T> currentConsumer;
	SettablePromise<Void> currentEndOfStream = new SettablePromise<>();

	private StreamConsumerSwitcher() {
	}

	public static <T> StreamConsumerSwitcher<T> create() {
		return new StreamConsumerSwitcher<>();
	}

	public void switchTo(@NotNull StreamConsumer<T> consumer) {
		//noinspection unused
		StreamConsumer<T> oldConsumer = this.currentConsumer;
		SettablePromise<Void> oldEndOfStream = this.currentEndOfStream;

		if (oldEndOfStream.isResult()) {
			StreamSupplier.<T>closing().streamTo(consumer);
			return;
		}

		if (oldEndOfStream.isException()) {
			StreamSupplier.<T>closingWithError(oldEndOfStream.getException()).streamTo(consumer);
			return;
		}

		this.currentConsumer = consumer;
		this.currentEndOfStream = new SettablePromise<>();

		oldEndOfStream.trySet(null);

		this.currentConsumer.getAcknowledgement()
				.whenComplete((v, e) -> {
					if (this.currentConsumer == consumer) {
						acknowledgement.trySet(v, e);
					}
				});

		if (!acknowledgement.isComplete() && supplier != null) {
			supplier.suspend();
			currentConsumer.consume(internalSupplier());
		}
	}

	@Override
	public void consume(@NotNull StreamSupplier<T> streamSupplier) {
		this.supplier = streamSupplier;
		this.supplier.getEndOfStream()
				.whenResult(this::endOfStream)
				.whenException(this::closeEx);
		if (getAcknowledgement().isComplete()) return;

		if (currentConsumer != null) {
			currentConsumer.consume(internalSupplier());
		}
	}

	@NotNull
	private StreamSupplier<T> internalSupplier() {
		return new StreamSupplier<T>() {
			final StreamConsumer<T> finalCurrentConsumer = currentConsumer;
			private final Promise<Void> endOfStream = Promise.ofCallback(currentEndOfStream::whenComplete);

			@Override
			public void resume(@Nullable StreamDataAcceptor<T> dataAcceptor) {
				checkArgument(finalCurrentConsumer == currentConsumer);
				assert StreamConsumerSwitcher.this.supplier != null;
				StreamConsumerSwitcher.this.supplier.resume(dataAcceptor);
			}

			@Override
			public Promise<Void> getEndOfStream() {
				return endOfStream;
			}

			@Override
			public void closeEx(@NotNull Throwable e) {
				StreamConsumerSwitcher.this.closeEx(e);
			}
		};
	}

	private void endOfStream() {
		if (currentConsumer == null) {
			acknowledgement.trySet(null);
		} else {
			currentEndOfStream.trySet(null);
			currentConsumer.getAcknowledgement()
					.whenResult(acknowledgement::trySet)
					.whenException(acknowledgement::trySetException);
		}
	}

	@Override
	public Promise<Void> getAcknowledgement() {
		return acknowledgement;
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
		acknowledgement.trySetException(e);
		if (currentConsumer != null) {
			currentConsumer.closeEx(e);
			currentEndOfStream.trySetException(e);
		}
	}

}
