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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * It is {@link AbstractStreamTransformer_1_N} which divides input stream  into groups with some key
 * function, and sends obtained streams to consumers.
 *
 * @param <T> type of input items
 */
@SuppressWarnings("unchecked")
public final class StreamSharder<T> implements HasInput<T>, HasOutputs, StreamDataReceiver<T> {
	private final Eventloop eventloop;
	private final Sharder<T> sharder;

	private final InputConsumer input;
	private final List<Output> outputs = new ArrayList<>();

	private StreamDataReceiver<T>[] dataReceivers = new StreamDataReceiver[0];
	private int suspended = 0;

	private StreamSharder(Eventloop eventloop, Sharder<T> sharder) {
		this.eventloop = eventloop;
		this.sharder = sharder;
		this.input = new InputConsumer(eventloop);
	}

	public static <T> StreamSharder<T> create(Eventloop eventloop, Sharder<T> sharder) {
		return new StreamSharder<T>(eventloop, sharder);
	}
	// endregion

	public StreamProducer<T> newOutput() {
		Output output = new Output(eventloop, outputs.size());
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
		public InputConsumer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onEndOfStream() {
			outputs.forEach(Output::sendEndOfStream);
		}

		@Override
		protected void onError(Exception e) {
			outputs.forEach(output -> output.closeWithError(e));
		}
	}

	protected final class Output extends AbstractStreamProducer<T> {
		private final int index;

		protected Output(Eventloop eventloop, int index) {
			super(eventloop);
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
		protected void onError(Exception e) {
			input.closeWithError(e);
		}
	}

}
