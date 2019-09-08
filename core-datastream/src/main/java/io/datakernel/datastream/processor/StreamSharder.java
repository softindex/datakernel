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
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * It is Stream Transformer which divides input stream  into groups with some key
 * function, and sends obtained streams to consumers.
 *
 * @param <T> type of input items
 */
@SuppressWarnings("unchecked")
public final class StreamSharder<T> implements StreamInput<T>, StreamOutputs, StreamDataAcceptor<T> {
	private final Sharder<T> sharder;

	private final InputConsumer input;
	private final List<Output> outputs = new ArrayList<>();

	private StreamDataAcceptor<T>[] dataAcceptors = new StreamDataAcceptor[0];
	private int suspended = 0;

	private StreamSharder(Sharder<T> sharder) {
		this.sharder = sharder;
		this.input = new InputConsumer();
	}

	public static <T> StreamSharder<T> create(Sharder<T> sharder) {
		return new StreamSharder<>(sharder);
	}
	// endregion

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
		int shard = sharder.shard(item);
		dataAcceptors[shard].accept(item);
	}

	protected final class InputConsumer extends AbstractStreamConsumer<T> {
		@Override
		protected Promise<Void> onEndOfStream() {
			return Promises.all(outputs.stream().map(Output::sendEndOfStream));
		}

		@Override
		protected void onError(Throwable e) {
			outputs.forEach(output -> output.close(e));
		}
	}

	protected final class Output extends AbstractStreamSupplier<T> {
		private final int index;

		Output(int index) {
			this.index = index;
		}

		@Override
		protected void onSuspended() {
			suspended++;
			input.getSupplier().suspend();
		}

		@Override
		protected void onProduce(StreamDataAcceptor<T> dataAcceptor) {
			dataAcceptors[index] = dataAcceptor;
			if (--suspended == 0) {
				input.getSupplier().resume(StreamSharder.this);
			}
		}

		@Override
		protected void onError(Throwable e) {
			input.close(e);
		}
	}

}
