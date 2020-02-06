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

package io.datakernel.datastream.stats;

import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.StreamDataSource;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamTransformer;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class StreamStatsForwarder<T> implements StreamTransformer<T, T> {
	private final Input input;
	private final Output output;

	private final StreamStats<T> stats;

	private StreamStatsForwarder(StreamStats<T> stats) {
		this.stats = stats;
		this.input = new Input();
		this.output = new Output();
		input.acknowledgement
				.whenException(output.endOfStream::trySetException);
		output.endOfStream
				.whenResult(input.acknowledgement::trySet)
				.whenException(input.acknowledgement::trySetException);
	}

	public static <T> StreamStatsForwarder<T> create(StreamStats<T> stats) {
		return new StreamStatsForwarder<>(stats);
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
			if (this.dataSource == null) {
				stats.onStarted();
			}
			this.dataSource = dataSource;
			if (input.dataSource != null) {
				input.dataSource.resume(output.dataAcceptor);
			}
		}

		@Override
		public void endOfStream() {
			if (output.endOfStream.trySet(null)) {
				stats.onEndOfStream();
			}
		}

		@Override
		public Promise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public void close(@NotNull Throwable e) {
			if (acknowledgement.trySetException(e)) {
				stats.onError(e);
			}
		}
	}

	protected final class Output implements StreamSupplier<T> {
		@Nullable StreamDataAcceptor<T> dataAcceptor;
		final SettablePromise<Void> endOfStream = new SettablePromise<>();

		@Override
		public void supply(@Nullable StreamDataAcceptor<T> dataAcceptor) {
			if (this.dataAcceptor == dataAcceptor) return;
			if (dataAcceptor != null) {
				stats.onProduce();
			} else {
				stats.onSuspend();
			}
			this.dataAcceptor = dataAcceptor;
			if (input.dataSource != null) {
				input.dataSource.resume(output.dataAcceptor);
			}
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public void close(@NotNull Throwable e) {
			if (endOfStream.trySetException(e)) {
				stats.onError(e);
			}
		}
	}

}
