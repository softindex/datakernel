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

package io.datakernel.stream.processor;

import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.util.Preconditions.checkState;

/**
 * Provides an ability to split stream into number of equivalent outputs.
 * {@link StreamSplitter} has one Input and an arbitrary number of Outputs.
 *
 * @param <T>
 */
@SuppressWarnings("unchecked")
public final class StreamSplitter<T> implements StreamInput<T>, StreamOutputs, StreamDataAcceptor<T> {
	private final Input input;
	private final List<Output> outputs = new ArrayList<>();

	@SuppressWarnings("unchecked")
	private StreamDataAcceptor<T>[] dataAcceptors = new StreamDataAcceptor[0];
	private int suspended = 0;


	// region creators
	private StreamSplitter() {
		input = new Input();
	}

	public static <T> StreamSplitter<T> create() {
		return new StreamSplitter<>();
	}
	//endregion

	public StreamSupplier<T> newOutput() {
		Output output = new Output(outputs.size());
		dataAcceptors = Arrays.copyOf(dataAcceptors, dataAcceptors.length + 1);
		suspended++;
		outputs.add(output);
		return output;
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public List<? extends StreamSupplier<T>> getOutputs() {
		return outputs;
	}

	@Override
	public void accept(T item) {
		//noinspection ForLoopReplaceableByForEach
		for (int i = 0; i < dataAcceptors.length; i++) {
			dataAcceptors[i].accept(item);
		}
	}

	protected final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			checkState(!outputs.isEmpty(), "Splitter has no outputs");
		}

		@Override
		protected Stage<Void> onEndOfStream() {
			return Stages.all(outputs.stream().map(Output::sendEndOfStream));
		}

		@Override
		protected void onError(Throwable t) {
			outputs.forEach(output -> output.closeWithError(t));
		}
	}

	protected final class Output extends AbstractStreamSupplier<T> {
		private final int index;

		protected Output(int index) {
			this.index = index;
		}

		@Override
		protected void onStarted() {
			checkState(input.getSupplier() != null, "Splitter has no input");
		}

		@Override
		protected void onSuspended() {
			suspended++;
			assert input.getSupplier() != null;
			input.getSupplier().suspend();
		}

		@Override
		protected void onProduce(StreamDataAcceptor<T> dataAcceptor) {
			dataAcceptors[index] = dataAcceptor;
			if (--suspended == 0) {
				assert input.getSupplier() != null;
				input.getSupplier().resume(StreamSplitter.this);
			}
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}
	}
}
