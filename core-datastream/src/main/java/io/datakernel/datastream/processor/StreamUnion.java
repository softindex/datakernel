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

import io.datakernel.datastream.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.common.Preconditions.checkState;

/**
 * It is Stream Transformer which unions all input streams and streams it
 * combination to the destination.
 *
 * @param <T> type of output data
 */
public final class StreamUnion<T> implements HasStreamOutput<T>, HasStreamInputs {
	private final List<Input> inputs = new ArrayList<>();
	private final Output output;
	private boolean started;

	public StreamUnion() {
		this.output = new Output();
	}

	public static <T> StreamUnion<T> create() {
		StreamUnion<T> union = new StreamUnion<>();
		Eventloop.getCurrentEventloop().post(union::start);
		return union;
	}

	@Override
	public List<? extends StreamConsumer<?>> getInputs() {
		return inputs;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}

	public StreamConsumer<T> newInput() {
		checkState(!started);
		Input input = new Input();
		inputs.add(input);
		input.acknowledgement
				.whenException(output.endOfStream::trySetException);
		output.endOfStream
				.whenResult(input.acknowledgement::trySet)
				.whenException(input.acknowledgement::trySetException);
		return input;
	}

	public void start() {
		checkState(!started);
		started = true;
		sync();
	}

	private void sync() {
		if (!started) return;
		if (output.endOfStream.isComplete()) return;
		if (inputs.stream().allMatch(input -> input.endOfStream)) {
			output.endOfStream.trySet(null);
		} else {
			for (Input input : inputs) {
				if (input.streamSupplier != null) {
					input.streamSupplier.resume(output.dataAcceptor);
				}
			}
		}
	}

	private final class Input implements StreamConsumer<T> {
		@Nullable StreamSupplier<T> streamSupplier;
		private boolean endOfStream;
		private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

		@Override
		public void consume(@NotNull StreamSupplier<T> streamSupplier) {
			this.streamSupplier = streamSupplier;
			this.streamSupplier.getEndOfStream()
					.whenResult(this::endOfStream)
					.whenException(this::closeEx);
			sync();
		}

		private void endOfStream() {
			endOfStream = true;
			sync();
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

	private final class Output implements StreamSupplier<T> {
		@Nullable StreamDataAcceptor<T> dataAcceptor;
		private final SettablePromise<Void> endOfStream = new SettablePromise<>();

		@Override
		public void resume(@Nullable StreamDataAcceptor<T> dataAcceptor) {
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

}
