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

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkState;

public final class ShardingStreamSplitter<I, K> implements StreamInput<I>, StreamOutputs, StreamDataAcceptor<I> {
	private final Input input;
	private final List<Output> outputs = new ArrayList<>();
	private final MultiSharder<K> sharder;
	private final Function<I, K> keyFunction;

	@SuppressWarnings("unchecked")
	private StreamDataAcceptor<I>[] dataAcceptors = new StreamDataAcceptor[0];
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

	public StreamSupplier<I> newOutput() {
		Output output = new Output(outputs.size());
		dataAcceptors = Arrays.copyOf(dataAcceptors, dataAcceptors.length + 1);
		suspended++;
		outputs.add(output);
		return output;
	}

	@Override
	public StreamConsumer<I> getInput() {
		return input;
	}

	@Override
	public List<? extends StreamSupplier<I>> getOutputs() {
		return outputs;
	}

	@Override
	public void accept(I item) {
		for (int index : sharder.shard(keyFunction.apply(item))) {
			dataAcceptors[index].accept(item);
		}
	}

	protected final class Input extends AbstractStreamConsumer<I> {
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

	protected final class Output extends AbstractStreamSupplier<I> {
		private final int index;

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
		protected void onProduce(StreamDataAcceptor<I> dataAcceptor) {
			dataAcceptors[index] = dataAcceptor;
			if (--suspended == 0) {
				input.getSupplier().resume(ShardingStreamSplitter.this);
			}
		}

		@Override
		protected void onError(Throwable e) {
			input.close(e);
		}
	}
}
