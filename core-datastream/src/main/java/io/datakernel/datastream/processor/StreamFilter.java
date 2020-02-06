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

package io.datakernel.datastream.processor;

import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.StreamDataSource;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Predicate;

/**
 * Provides you apply function before sending data to the destination. It is a {@link StreamFilter}
 * which receives specified type and streams set of function's result  to the destination .
 */
public final class StreamFilter<T> implements StreamTransformer<T, T> {
	private final Predicate<T> predicate;
	private final Input input;
	private final Output output;

	private StreamFilter(Predicate<T> predicate) {
		this.predicate = predicate;
		this.input = new Input();
		this.output = new Output();
		input.acknowledgement
				.whenException(output.endOfStream::trySetException);
		output.endOfStream
				.whenResult(() -> input.acknowledgement.trySet(null))
				.whenException(input.acknowledgement::trySetException);
	}

	public static <T> StreamFilter<T> create(Predicate<T> predicate) {
		return new StreamFilter<>(predicate);
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}

	protected final class Input implements StreamConsumer<T> {
		@Nullable StreamDataSource<T> dataSource;
		final SettablePromise<Void> acknowledgement = new SettablePromise<>();

		@Override
		public void consume(@NotNull StreamDataSource<T> dataSource) {
			this.dataSource = dataSource;
			sync();
		}

		@Override
		public void endOfStream() {
			output.endOfStream.trySet(null);
		}

		@Override
		public Promise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public void close(@NotNull Throwable e) {
			acknowledgement.trySetException(e);
		}
	}

	protected final class Output implements StreamSupplier<T> {
		@Nullable StreamDataAcceptor<T> dataAcceptor;
		final SettablePromise<Void> endOfStream = new SettablePromise<>();

		@Override
		public void supply(@Nullable StreamDataAcceptor<T> dataAcceptor) {
			if (this.dataAcceptor == dataAcceptor) return;
			this.dataAcceptor = dataAcceptor;
			sync();
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public void close(@NotNull Throwable e) {
			endOfStream.trySetException(e);
		}
	}

	private void sync() {
		if (input.dataSource != null) {
			final Predicate<T> predicate = this.predicate;
			final StreamDataAcceptor<T> dataAcceptor = output.dataAcceptor;
			if (dataAcceptor != null) {
				input.dataSource.resume(item -> {
					if (predicate.test(item)) {
						dataAcceptor.accept(item);
					}
				});
			} else {
				input.dataSource.suspend();
			}
		}
	}

}
