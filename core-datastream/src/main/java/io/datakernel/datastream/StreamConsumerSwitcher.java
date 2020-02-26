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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A consumer that wraps around another consumer that can be hot swapped with some other consumer.
 * <p>
 * It sets its acknowledgement on supplier end of stream, and acts as if suspended when current consumer stops and acknowledges.
 */
public final class StreamConsumerSwitcher<T> extends AbstractStreamConsumer<T> {
	private @Nullable InternalStreamSupplier currentSupplier;
	private @Nullable StreamConsumer<T> currentConsumer;

	private StreamConsumerSwitcher() {
	}

	/**
	 * Creates a new instance of this consumer.
	 */
	public static <T> StreamConsumerSwitcher<T> create() {
		return new StreamConsumerSwitcher<>();
	}

	public void switchTo(@NotNull StreamConsumer<T> consumer) {
		if (isEndOfStream()) {
			StreamSupplier.<T>closing()
					.streamTo(consumer);
			return;
		}

		if (getAcknowledgement().isException()) {
			StreamSupplier.<T>closingWithError(getAcknowledgement().getException())
					.streamTo(consumer);
			return;
		}

		InternalStreamSupplier oldSupplier = this.currentSupplier;

		this.currentSupplier = new InternalStreamSupplier();
		this.currentConsumer = consumer;

		if (oldSupplier != null) {
			oldSupplier.sendEndOfStream();
		}

		this.currentConsumer.getAcknowledgement()
				.whenComplete((v, e) -> {
					if (this.currentConsumer == consumer) {
						acknowledgement.trySet(v, e);
					}
				});

		currentSupplier.streamTo(currentConsumer);
	}

	@Override
	protected void onStarted() {
		if (currentSupplier != null && currentSupplier.getDataAcceptor() != null) {
			resume(currentSupplier.getDataAcceptor());
		}
	}

	@Override
	protected void onEndOfStream() {
		if (currentSupplier == null) {
			acknowledge();
		} else {
			currentSupplier.sendEndOfStream();
			currentConsumer.getAcknowledgement()
					.whenResult(this::acknowledge)
					.whenException(this::closeEx);
		}
	}

	@Override
	protected void onError(Throwable e) {
		if (currentSupplier != null) {
			currentSupplier.closeEx(e);
		}
	}

	private class InternalStreamSupplier extends AbstractStreamSupplier<T> {
		@Override
		protected void onResumed(AsyncProduceController async) {
			StreamConsumerSwitcher.this.resume(getDataAcceptor());
		}

		@Override
		protected void onSuspended() {
			StreamConsumerSwitcher.this.suspend();
		}

		@Override
		protected void onError(Throwable e) {
			StreamConsumerSwitcher.this.closeEx(e);
		}
	}
}
