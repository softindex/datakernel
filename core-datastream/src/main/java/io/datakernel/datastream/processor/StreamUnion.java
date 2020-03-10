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
import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.common.Preconditions.checkState;

/**
 * It is Stream Transformer which unions all input streams and streams it
 * combination to the destination.
 *
 * @param <T> type of output data
 */
public final class StreamUnion<T> implements HasStreamOutput<T>, HasStreamInputs {
	private final List<Input> inputs = new ArrayList<>();
	private final Output output;
	private boolean started;

	public StreamUnion() {
		this.output = new Output();
	}

	public static <T> StreamUnion<T> create() {
		StreamUnion<T> union = new StreamUnion<>();
		Eventloop.getCurrentEventloop().post(union::start);
		return union;
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
		checkState(!started);
		Input input = new Input();
		inputs.add(input);
		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getEndOfStream()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
		return input;
	}

	public void start() {
		checkState(!started);
		started = true;
		sync();
	}

	private final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			sync();
		}

		@Override
		protected void onEndOfStream() {
			sync();
		}
	}

	private final class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onResumed() {
			sync();
		}

		@Override
		protected void onSuspended() {
			sync();
		}
	}

	private void sync() {
		if (!started) return;
		if (inputs.stream().allMatch(Input::isEndOfStream)) {
			output.sendEndOfStream();
			return;
		}
		for (Input input : inputs) {
			input.resume(output.getDataAcceptor());
		}
	}

}
