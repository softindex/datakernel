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

/**
 * It is {@link AbstractStreamTransformer_1_N} which divides input stream  into groups with some key
 * function, and sends obtained streams to consumers.
 *
 * @param <T> type of input items
 */
@SuppressWarnings("unchecked")
public final class StreamSharder<T> implements StreamInput<T>, StreamOutputs, StreamDataReceiver<T> {
	private final Sharder<T> sharder;

	private final InputConsumer input;
	private final List<Output> outputs = new ArrayList<>();

	private StreamDataReceiver<T>[] dataReceivers = new StreamDataReceiver[0];
	private int suspended = 0;

	private StreamSharder(Sharder<T> sharder) {
		this.sharder = sharder;
		this.input = new InputConsumer();
	}

	public static <T> StreamSharder<T> create(Sharder<T> sharder) {
		return new StreamSharder<>(sharder);
	}
	// endregion

	public StreamProducer<T> newOutput() {
		Output output = new Output(outputs.size());
		dataReceivers = Arrays.copyOf(dataReceivers, dataReceivers.length + 1);
		suspended++;
		outputs.add(output);
		return output;
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public List<? extends StreamProducer<T>> getOutputs() {
		return outputs;
	}

	@Override
	public void onData(T item) {
		int shard = sharder.shard(item);
		dataReceivers[shard].onData(item);
	}

	protected final class InputConsumer extends AbstractStreamConsumer<T> {
		@Override
		protected Stage<Void> onProducerEndOfStream() {
			return Stages.all(outputs.stream().map(Output::sendEndOfStream));
		}

		@Override
		protected void onError(Throwable t) {
			outputs.forEach(output -> output.closeWithError(t));
		}
	}

	protected final class Output extends AbstractStreamProducer<T> {
		private final int index;

		protected Output(int index) {
			this.index = index;
		}

		@Override
		protected void onSuspended() {
			suspended++;
			input.getProducer().suspend();
		}

		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			dataReceivers[index] = dataReceiver;
			if (--suspended == 0) {
				input.getProducer().produce(StreamSharder.this);
			}
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}
	}

}
