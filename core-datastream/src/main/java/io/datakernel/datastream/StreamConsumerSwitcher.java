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

/**
 * Provides you apply function before sending data to the destination. It is a {@link StreamConsumerSwitcher}
 * which receives specified type and streams set of function's result  to the destination .
 */
public final class StreamConsumerSwitcher<T> implements StreamConsumer<T> {
	@Nullable
	private StreamConsumer<T> consumer;

	private boolean endOfStream;

	private @Nullable StreamDataSource<T> dataSource;
	private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

	private StreamConsumerSwitcher() {
	}

	public static <T> StreamConsumerSwitcher<T> create() {
		return new StreamConsumerSwitcher<>();
	}

	public void switchTo(@NotNull StreamConsumer<T> consumer) {
		StreamConsumer<T> oldConsumer = this.consumer;
		if (this.endOfStream) {
			if (!consumer.getAcknowledgement().isComplete()) {
				consumer.endOfStream();
			}
			return;
		}
		this.consumer = consumer;

		if (oldConsumer != null && !oldConsumer.getAcknowledgement().isComplete()) {
			oldConsumer.endOfStream();
		}

		this.consumer.getAcknowledgement()
				.whenResult(result -> {
					if (this.consumer == consumer) {
						acknowledgement.trySet(result);
					}
				})
				.whenException(e -> {
					if (this.consumer == consumer) {
						acknowledgement.trySetException(e);
					}
				});

		if (!acknowledgement.isComplete() && dataSource != null) {
			dataSource.suspend();
			this.consumer.consume(dataAcceptor -> {
				if (this.consumer == consumer) {
					dataSource.resume(dataAcceptor);
				}
			});
		}
	}

	@Override
	public void consume(@NotNull StreamDataSource<T> dataSource) {
		this.dataSource = dataSource;
		if (consumer != null) {
			final StreamConsumer<T> consumer = this.consumer;
			this.consumer.consume(dataAcceptor -> {
				if (this.consumer == consumer) {
					this.dataSource.resume(dataAcceptor);
				}
			});
		}
	}

	@Override
	public void endOfStream() {
		this.endOfStream = true;
		if (consumer == null) {
			this.acknowledgement.trySet(null);
		} else {
			consumer.endOfStream();
			consumer.getAcknowledgement()
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
		if (consumer != null) {
			consumer.closeEx(e);
		}
	}

}
