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

import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkState;

public final class ShardingStreamSplitter<I, K> implements HasInput<I>, HasOutputs, StreamDataReceiver<I> {
	private final Input input;
	private final List<Output> outputs = new ArrayList<>();
	private final MultiSharder<K> sharder;
	private final Function<I, K> keyFunction;

	@SuppressWarnings("unchecked")
	private StreamDataReceiver<I>[] dataReceivers = new StreamDataReceiver[0];
	private int suspended = 0;

	private ShardingStreamSplitter(MultiSharder<K> sharder, Function<I, K> keyFunction) {
		this.sharder = sharder;
		this.keyFunction = keyFunction;
		this.input = new Input();
	}

	public static <T> ShardingStreamSplitter<T, T> create(MultiSharder<T> sharder) {
		return new ShardingStreamSplitter<>(sharder, Function.identity());
	}

	public static <I, K> ShardingStreamSplitter<I, K> create(MultiSharder<K> sharder, Function<I, K> keyFunction) {
		return new ShardingStreamSplitter<>(sharder, keyFunction);
	}

	public StreamProducer<I> newOutput() {
		Output output = new Output(outputs.size());
		dataReceivers = Arrays.copyOf(dataReceivers, dataReceivers.length + 1);
		suspended++;
		outputs.add(output);
		return output;
	}

	@Override
	public StreamConsumer<I> getInput() {
		return input;
	}

	@Override
	public List<? extends StreamProducer<I>> getOutputs() {
		return outputs;
	}

	@Override
	public void onData(I item) {
		for (int index : sharder.shard(keyFunction.apply(item))) {
			dataReceivers[index].onData(item);
		}
	}

	protected final class Input extends AbstractStreamConsumer<I> {
		@Override
		protected void onStarted() {
			checkState(!outputs.isEmpty(), "Empty outputs");
		}

		@Override
		protected void onEndOfStream() {
			outputs.forEach(Output::sendEndOfStream);
		}

		@Override
		protected void onError(Throwable t) {
			outputs.forEach(output -> output.closeWithError(t));
		}
	}

	protected final class Output extends AbstractStreamProducer<I> {
		private final int index;

		protected Output(int index) {
			this.index = index;
		}

		@Override
		protected void onStarted() {
			checkState(input.getProducer() != null, "Splitter has no input");
		}

		@Override
		protected void onSuspended() {
			suspended++;
			input.getProducer().suspend();
		}

		@Override
		protected void onProduce(StreamDataReceiver<I> dataReceiver) {
			dataReceivers[index] = dataReceiver;
			if (--suspended == 0) {
				input.getProducer().produce(ShardingStreamSplitter.this);
			}
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}
	}
}
