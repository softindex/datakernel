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

package io.datakernel.stream;

import io.datakernel.eventloop.Eventloop;

/**
 * Provides you apply function before sending data to the destination. It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams set of function's result  to the destination .
 *
 * @param <T> type of data
 */
public final class StreamForwarder<T> extends AbstractStreamTransformer_1_1<T, T> {

	private final InputConsumer inputConsumer;
	private final OutputProducer outputProducer;

	protected final class InputConsumer extends AbstractInputConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.sendEndOfStream();
		}

		@SuppressWarnings("unchecked")
		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return outputProducer.getDownstreamDataReceiver();
		}
	}

	protected final class OutputProducer extends AbstractOutputProducer {

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			inputConsumer.resume();
		}
	}

	private StreamForwarder(Eventloop eventloop) {
		super(eventloop);
		this.inputConsumer = new InputConsumer();
		this.outputProducer = new OutputProducer();
	}

	public static <T> StreamForwarder<T> create(Eventloop eventloop) {return new StreamForwarder<>(eventloop);}
}
