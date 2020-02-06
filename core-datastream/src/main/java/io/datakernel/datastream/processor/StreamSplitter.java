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
import io.datakernel.promise.Promises;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.datakernel.common.Preconditions.checkState;

/**
 * It is Stream Transformer which divides input stream  into groups with some key
 * function, and sends obtained streams to consumers.
 *
 * @param <I> type of input items
 * @param <O> type of output items
 */
@SuppressWarnings("unchecked")
public final class StreamSplitter<I, O> implements HasStreamInput<I>, HasStreamOutputs<O> {
	private final Function<StreamDataAcceptor<O>[], StreamDataAcceptor<I>> acceptorFactory;
	private final InputConsumer input;
	private final List<Output> outputs = new ArrayList<>();

	private StreamDataAcceptor<O>[] dataAcceptors = new StreamDataAcceptor[8];
	private int active;

	private boolean started;

	private StreamSplitter(Function<StreamDataAcceptor<O>[], StreamDataAcceptor<I>> acceptorFactory) {
		this.acceptorFactory = acceptorFactory;
		this.input = new InputConsumer();
	}

	public static <I, O> StreamSplitter<I, O> create(BiConsumer<I, StreamDataAcceptor<O>[]> action) {
		return create(acceptors -> item -> action.accept(item, acceptors));
	}

	public static <I, O> StreamSplitter<I, O> create(Function<StreamDataAcceptor<O>[], StreamDataAcceptor<I>> acceptorFactory) {
		StreamSplitter<I, O> streamSplitter = new StreamSplitter<>(acceptorFactory);
		Eventloop.getCurrentEventloop().post(streamSplitter::start);
		return streamSplitter;
	}

	public StreamSupplier<O> newOutput() {
		checkState(!started);
		Output output = new Output(outputs.size());
		outputs.add(output);
		if (outputs.size() > dataAcceptors.length) {
			dataAcceptors = Arrays.copyOf(dataAcceptors, dataAcceptors.length * 2);
		}
		return output;
	}

	@Override
	public StreamConsumer<I> getInput() {
		return input;
	}

	@Override
	public List<? extends StreamSupplier<O>> getOutputs() {
		return outputs;
	}

	public void start() {
		checkState(!started);
		started = true;
		dataAcceptors = Arrays.copyOf(dataAcceptors, outputs.size());
		input.acknowledgement
				.whenException(e -> outputs.forEach(output -> output.endOfStream.trySetException(e)));
		Promises.all(outputs.stream().map(Output::getEndOfStream))
				.whenResult(input.acknowledgement::trySet)
				.whenException(input.acknowledgement::trySetException);
		sync();
	}

	private void sync() {
		if (!started) return;
		if (input.acknowledgement.isComplete()) return;
		if (input.dataSource != null) {
			if (active == dataAcceptors.length) {
				input.dataSource.resume(acceptorFactory.apply(this.dataAcceptors));
			} else {
				input.dataSource.suspend();
			}
		}
	}

	protected final class InputConsumer implements StreamConsumer<I> {
		@Nullable StreamDataSource<I> dataSource;
		final SettablePromise<Void> acknowledgement = new SettablePromise<>();

		@Override
		public void consume(@NotNull StreamDataSource<I> dataSource) {
			this.dataSource = dataSource;
			sync();
		}

		@Override
		public void endOfStream() {
			for (Output output : outputs) {
				output.endOfStream.trySet(null);
			}
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

	protected final class Output implements StreamSupplier<O> {
		final int index;
		@Nullable StreamDataAcceptor<O> dataAcceptor;
		final SettablePromise<Void> endOfStream = new SettablePromise<>();

		public Output(int index) {this.index = index;}

		@Override
		public void supply(@Nullable StreamDataAcceptor<O> dataAcceptor) {
			if (dataAcceptor != null && this.dataAcceptor == null) active++;
			if (dataAcceptor == null && this.dataAcceptor != null) active--;
			if (this.dataAcceptor == dataAcceptor) return;
			this.dataAcceptor = dataAcceptor;
			dataAcceptors[index] = dataAcceptor;
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

}
