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

import java.util.ArrayList;
import java.util.List;

/**
 * It is Stream Transformer which unions all input streams and streams it
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
		return new StreamUnion<>();
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
			assert output.getConsumer() != null;
			return output.getConsumer().getAcknowledgement();
		}

		@Override
		protected void onError(Throwable e) {
			output.close(e);
		}
	}

	private final class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onSuspended() {
			for (Input input : inputs) {
				input.getSupplier().suspend();
			}
		}

		@Override
		protected void onProduce(StreamDataAcceptor<T> dataAcceptor) {
			if (!inputs.isEmpty()) {
				for (Input input : inputs) {
					input.getSupplier().resume(dataAcceptor);
				}
			} else {
				eventloop.post(this::sendEndOfStream);
			}
		}

		@Override
		protected void onError(Throwable e) {
			inputs.forEach(input -> input.close(e));
		}
	}

}
