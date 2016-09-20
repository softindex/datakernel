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
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides you apply function before sending data to the destination. It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams set of function's result  to the destination .
 *
 * @param <I> type of input data
 * @param <O> type of output data
 */
public final class StreamFunction<I, O> extends AbstractStreamTransformer_1_1<I, O> {
	private final InputConsumer inputConsumer;
	private final OutputProducer outputProducer;

	// region creators
	private StreamFunction(Eventloop eventloop, Function<I, O> function) {
		super(eventloop);
		this.inputConsumer = new InputConsumer();
		this.outputProducer = new OutputProducer(function);
	}

	public static <I, O> StreamFunction<I, O> create(Eventloop eventloop, Function<I, O> function) {
		return new StreamFunction<I, O>(eventloop, function);
	}
	// endregion

	protected final class InputConsumer extends AbstractInputConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.sendEndOfStream();
		}

		@SuppressWarnings("unchecked")
		@Override
		public StreamDataReceiver<I> getDataReceiver() {
			return outputProducer.function == Functions.identity() ?
					(StreamDataReceiver<I>) outputProducer.getDownstreamDataReceiver() :
					outputProducer;
		}
	}

	protected final class OutputProducer extends AbstractOutputProducer implements StreamDataReceiver<I> {
		private final Function<I, O> function;

		public OutputProducer(Function<I, O> function) {this.function = checkNotNull(function);}

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			inputConsumer.resume();
		}

		@Override
		public void onData(I item) {
			send(function.apply(item));
		}
	}
}
