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
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

/**
 * Provides you apply function before sending data to the destination. It is a {@link StreamMapper}
 * which receives specified type and streams set of function's result  to the destination .
 *
 * @param <I> type of input data
 * @param <O> type of output data
 */
public final class StreamMapper<I, O> implements StreamTransformer<I, O> {
	private final Function<I, O> function;
	private final Input input;
	private final Output output;

	private StreamMapper(Function<I, O> function) {
		this.function = function;
		this.input = new Input();
		this.output = new Output();
		input.acknowledgement
				.whenException(output.endOfStream::trySetException);
		output.endOfStream
				.whenResult(input.acknowledgement::trySet)
				.whenException(input.acknowledgement::trySetException);
	}

	public static <I, O> StreamMapper<I, O> create(Function<I, O> function) {
		return new StreamMapper<>(function);
	}

	@Override
	public StreamConsumer<I> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<O> getOutput() {
		return output;
	}

	protected final class Input implements StreamConsumer<I> {
		@Nullable StreamSupplier<I> streamSupplier;
		final SettablePromise<Void> acknowledgement = new SettablePromise<>();

		@Override
		public void consume(@NotNull StreamSupplier<I> streamSupplier) {
			this.streamSupplier = streamSupplier;
			this.streamSupplier.getEndOfStream()
					.whenResult(this::endOfStream)
					.whenException(this::closeEx);
			sync();
		}

		private void endOfStream() {
			output.endOfStream.trySet(null);
		}

		@Override
		public Promise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
			acknowledgement.trySetException(e);
		}
	}

	protected final class Output implements StreamSupplier<O> {
		@Nullable StreamDataAcceptor<O> dataAcceptor;
		final SettablePromise<Void> endOfStream = new SettablePromise<>();

		@Override
		public void resume(@Nullable StreamDataAcceptor<O> dataAcceptor) {
			if (this.dataAcceptor == dataAcceptor) return;
			this.dataAcceptor = dataAcceptor;
			sync();
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
			endOfStream.trySetException(e);
		}
	}

	private void sync() {
		if (input.streamSupplier != null) {
			final Function<I, O> function = this.function;
			final StreamDataAcceptor<O> dataAcceptor = output.dataAcceptor;
			if (dataAcceptor != null) {
				input.streamSupplier.resume(item -> dataAcceptor.accept(function.apply(item)));
			} else {
				input.streamSupplier.suspend();
			}
		}
	}

}
