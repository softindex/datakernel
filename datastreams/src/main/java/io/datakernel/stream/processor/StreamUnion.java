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
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.List;

/**
 * It is {@link AbstractStreamTransformer_1_1} which unions all input streams and streams it
 * combination to the destination.
 *
 * @param <T> type of output data
 */
public final class StreamUnion<T> implements StreamOutput<T>, StreamInputs {
	private final List<Input> inputs = new ArrayList<>();
	private final Output output;

	// region creators
	private StreamUnion() {
		this.output = new Output();
	}

	public static <T> StreamUnion<T> create() {
		return new StreamUnion<T>();
	}

	@Override
	public List<? extends StreamConsumer<?>> getInputs() {
		return inputs;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}

	public StreamConsumer<T> newInput() {
		Input input = new Input();
		inputs.add(input);
		return input;
	}

	// endregion

	private final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected Promise<Void> onEndOfStream() {
			if (inputs.stream().allMatch(input -> input.getEndOfStream().isResult())) {
				output.sendEndOfStream();
			}
			return output.getConsumer().getAcknowledgement();
		}

		@Override
		protected void onError(Throwable t) {
			output.close(t);
		}
	}

	private final class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onSuspended() {
			for (int i = 0; i < inputs.size(); i++) {
				inputs.get(i).getSupplier().suspend();
			}
		}

		@Override
		protected void onProduce(StreamDataAcceptor<T> dataAcceptor) {
			if (!inputs.isEmpty()) {
				for (int i = 0; i < inputs.size(); i++) {
					inputs.get(i).getSupplier().resume(dataAcceptor);
				}
			} else {
				eventloop.post(this::sendEndOfStream);
			}
		}

		@Override
		protected void onError(Throwable t) {
			inputs.forEach(input -> input.close(t));
		}
	}

}
