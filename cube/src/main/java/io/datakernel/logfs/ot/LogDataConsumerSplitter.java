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

package io.datakernel.logfs.ot;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.PromisesAccumulator;
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.datakernel.util.Preconditions.checkState;

@SuppressWarnings("unchecked")
public abstract class LogDataConsumerSplitter<T, D> implements LogDataConsumer<T, D> {
	private final List<LogDataConsumer<?, D>> logDataConsumers = new ArrayList<>();
	private Iterator<? extends StreamDataAcceptor<?>> receivers;

	@Override
	public StreamConsumerWithResult<T, List<D>> consume() {
		if (logDataConsumers.isEmpty()) {
			createSplitter(); // recording scheme
			checkState(!logDataConsumers.isEmpty(), "addOutput() should be called at least once");
		}
		PromisesAccumulator<List<D>> resultsReducer = PromisesAccumulator.create(new ArrayList<>());
		Splitter splitter = new Splitter();
		for (LogDataConsumer<?, D> logDataConsumer : logDataConsumers) {
			StreamConsumerWithResult<?, List<D>> consumer = logDataConsumer.consume();
			resultsReducer.addPromise(consumer.getResult(), List::addAll);
			Splitter.Output<?> output = splitter.new Output<>();
			splitter.outputs.add(output);
			output.streamTo((StreamConsumer) consumer.getConsumer());
		}
		return StreamConsumerWithResult.of(splitter.getInput(), resultsReducer.get());
	}

	protected abstract StreamDataAcceptor<T> createSplitter();

	protected final <X> StreamDataAcceptor<X> addOutput(LogDataConsumer<X, D> logDataConsumer) {
		if (receivers == null) {
			// initial run, recording scheme
			logDataConsumers.add(logDataConsumer);
			return null;
		}
		// receivers must correspond outputs for recorded scheme
		return (StreamDataAcceptor<X>) receivers.next();
	}

	final class Splitter implements StreamInput<T>, StreamOutputs {
		private final Input input;
		private final List<Output<?>> outputs = new ArrayList<>();

		private StreamDataAcceptor<T> inputAcceptor;

		private int ready = 0;

		protected Splitter() {
			this.input = new Input();
		}

		@Override
		public StreamConsumer<T> getInput() {
			return input;
		}

		@Override
		public List<? extends StreamSupplier<?>> getOutputs() {
			return outputs;
		}

		final class Input extends AbstractStreamConsumer<T> {
			@Override
			protected Promise<Void> onEndOfStream() {
				return Promises.all(outputs.stream().map(Output::sendEndOfStream));
			}

			@Override
			protected void onError(Throwable t) {
				outputs.forEach(output -> output.close(t));
			}
		}

		final class Output<X> extends AbstractStreamSupplier<X> {
			private StreamDataAcceptor<?> dataAcceptor;

			@Override
			protected void onProduce(StreamDataAcceptor<X> dataAcceptor) {
				this.dataAcceptor = dataAcceptor;
				if (++ready == outputs.size()) {
					if (inputAcceptor == null) {
						receivers = outputs.stream().map(output -> output.dataAcceptor).iterator();
						inputAcceptor = createSplitter();
						checkState(!receivers.hasNext());
						receivers = null;
					}
					input.getSupplier().resume(inputAcceptor);
				}
			}

			@Override
			protected void onSuspended() {
				--ready;
				input.getSupplier().suspend();
			}

			@Override
			protected void onError(Throwable t) {
				input.close(t);
			}
		}
	}
}
