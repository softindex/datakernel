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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.util.Preconditions.checkState;

@SuppressWarnings("unchecked")
public final class FailsafeStreamSplitter<T> implements HasInput<T>, HasOutputs, StreamDataReceiver<T> {
	private final Input input;
	private final List<Output> outputs = new ArrayList<>();

	private StreamDataReceiver<T>[] dataReceivers = new StreamDataReceiver[0];
	private int suspended = 0;

	private List<Throwable> failures = new ArrayList<>();

	private FailsafeStreamSplitter() {
		input = new Input();
	}

	public static <T> FailsafeStreamSplitter<T> create() {
		return new FailsafeStreamSplitter<>();
	}

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
		for (StreamDataReceiver<T> receiver : dataReceivers) {
			if (receiver != null) {
				receiver.onData(item);
			}
		}
	}

	protected final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			checkState(!outputs.isEmpty(), "Splitter has no outputs");
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

	protected final class Output extends AbstractStreamProducer<T> {
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
			assert input.getProducer() != null;
			input.getProducer().suspend();
		}

		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			dataReceivers[index] = dataReceiver;
			if (--suspended == 0) {
				assert input.getProducer() != null;
				input.getProducer().produce(FailsafeStreamSplitter.this);
			}
		}

		@Override
		protected void onError(Throwable t) {
			failures.add(t);
			if (failures.size() == dataReceivers.length) {
				Throwable compoundError = new IOException("All streams were closed with error");
				for (Throwable failure : failures) {
					compoundError.addSuppressed(failure);
				}
				input.closeWithError(compoundError);
			} else if (!isReceiverReady() && --suspended == 0) {
				assert input.getProducer() != null;
				input.getProducer().produce(FailsafeStreamSplitter.this);
			}
		}
	}
}
