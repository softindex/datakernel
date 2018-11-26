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

package io.datakernel.stream.processor;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
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
public final class StreamSplitter<T> implements StreamInput<T>, StreamOutputs, StreamDataAcceptor<T> {
	private final Input input;
	private final List<Output> outputs = new ArrayList<>();

	@SuppressWarnings("unchecked")
	private StreamDataAcceptor<T>[] dataAcceptors = new StreamDataAcceptor[0];
	private int suspended = 0;

	private boolean lenient = false;
	private List<Throwable> lenientExceptions = new ArrayList<>();

	private StreamSplitter() {
		input = new Input();
	}

	public static <T> StreamSplitter<T> create() {
		return new StreamSplitter<>();
	}

	public StreamSplitter<T> lenient() {
		lenient = true;
		return this;
	}

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
		for (StreamDataAcceptor<T> dataAcceptor : dataAcceptors) {
			if (dataAcceptor != null) {
				dataAcceptor.accept(item);
			}
		}
	}

	final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			checkState(!outputs.isEmpty(), "Splitter has no outputs");
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

	final class Output extends AbstractStreamSupplier<T> {
		private final int index;
		private boolean isSuspended = false;

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
			isSuspended = true;
			assert input.getSupplier() != null;
			input.getSupplier().suspend();
		}

		@Override
		protected void onProduce(StreamDataAcceptor<T> dataAcceptor) {
			dataAcceptors[index] = dataAcceptor;
			isSuspended = false;
			if (--suspended == 0) {
				assert input.getSupplier() != null;
				input.getSupplier().resume(StreamSplitter.this);
			}
		}

		@Override
		protected void onError(Throwable e) {
			if (!lenient) {
				input.close(e);
				return;
			}
			dataAcceptors[index] = null;
			if (isSuspended) {
				suspended--;
			}
			outputs.remove(this);
			if (!outputs.isEmpty()) {
				lenientExceptions.add(e);
				return;
			}
			lenientExceptions.forEach(e::addSuppressed);
			input.close(e);
		}
	}
}
