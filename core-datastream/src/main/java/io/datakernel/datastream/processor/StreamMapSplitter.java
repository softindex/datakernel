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

package io.datakernel.datastream.processor;

import io.datakernel.datastream.*;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static io.datakernel.common.Preconditions.checkState;

public final class StreamMapSplitter<I> implements StreamInput<I>, StreamOutputs, StreamDataAcceptor<I> {
	private final Input input;
	private final List<Output> outputs = new ArrayList<>();
	private final BiConsumer<I, StreamDataAcceptor<Object>[]> action;

	@SuppressWarnings("unchecked")
	private StreamDataAcceptor<Object>[] dataAcceptors = new StreamDataAcceptor[0];
	private int suspended = 0;

	private StreamMapSplitter(BiConsumer<I, StreamDataAcceptor<Object>[]> action) {
		this.action = action;
		this.input = new Input();
	}

	public static <I> StreamMapSplitter<I> create(BiConsumer<I, StreamDataAcceptor<Object>[]> action) {
		return new StreamMapSplitter<>(action);
	}

	@SuppressWarnings("unchecked")
	public <O> StreamSupplier<O> newOutput() {
		Output output = new Output(outputs.size());
		dataAcceptors = Arrays.copyOf(dataAcceptors, dataAcceptors.length + 1);
		suspended++;
		outputs.add(output);
		return (StreamSupplier<O>) output;
	}

	@Override
	public StreamConsumer<I> getInput() {
		return input;
	}

	@Override
	public List<? extends StreamSupplier<?>> getOutputs() {
		return outputs;
	}

	@Override
	public void accept(I item) {
		action.accept(item, dataAcceptors);
	}

	final class Input extends AbstractStreamConsumer<I> {
		@Override
		protected void onStarted() {
			checkState(!outputs.isEmpty(), "Empty outputs");
		}

		@Override
		protected Promise<Void> onEndOfStream() {
			return Promises.all(outputs.stream().map(Output::sendEndOfStream));
		}

		@Override
		protected void onError(Throwable e) {
			outputs.forEach(output -> output.close(e));
		}
	}

	final class Output extends AbstractStreamSupplier<Object> {
		final int index;

		Output(int index) {
			this.index = index;
		}

		@Override
		protected void onStarted() {
			checkState(input.getSupplier() != null, "Splitter has no input");
		}

		@Override
		protected void onSuspended() {
			suspended++;
			input.getSupplier().suspend();
		}

		@Override
		protected void onProduce(StreamDataAcceptor<Object> dataAcceptor) {
			dataAcceptors[index] = dataAcceptor;
			if (--suspended == 0) {
				input.getSupplier().resume(StreamMapSplitter.this);
			}
		}

		@Override
		protected void onError(Throwable e) {
			input.close(e);
		}
	}
}
