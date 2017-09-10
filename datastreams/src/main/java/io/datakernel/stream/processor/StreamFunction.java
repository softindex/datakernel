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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

/**
 * Provides you apply function before sending data to the destination. It is a {@link StreamFunction}
 * which receives specified type and streams set of function's result  to the destination .
 *
 * @param <I> type of input data
 * @param <O> type of output data
 */
public final class StreamFunction<I, O> implements StreamTransformer<I, O> {
	private final Eventloop eventloop;
	private final Function<I, O> function;
	private final Input input;
	private final Output output;

	// region creators
	private StreamFunction(Eventloop eventloop, Function<I, O> function) {
		this.eventloop = eventloop;
		this.function = function;
		this.input = new Input(eventloop);
		this.output = new Output(eventloop);
	}

	public static <I, O> StreamFunction<I, O> create(Eventloop eventloop, Function<I, O> function) {
		return new StreamFunction<I, O>(eventloop, function);
	}

	@Override
	public StreamConsumer<I> getInput() {
		return input;
	}

	@Override
	public StreamProducer<O> getOutput() {
		return output;
	}
	// endregion

	protected final class Input extends AbstractStreamConsumer<I> {
		protected Input(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
		}

		@Override
		protected void onError(Exception e) {
			output.closeWithError(e);
		}
	}

	protected final class Output extends AbstractStreamProducer<O> {
		protected Output(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onSuspended() {
			input.getProducer().suspend();
		}

		@Override
		protected void onError(Exception e) {
			input.closeWithError(e);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void onProduce(StreamDataReceiver<O> dataReceiver) {
			input.getProducer().produce(
					function == Functions.identity() ?
							(StreamDataReceiver<I>) dataReceiver :
							item -> dataReceiver.onData(function.apply(item)));
		}
	}
}
