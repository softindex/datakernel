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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.AbstractStreamTransformer_N_1;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;

/**
 * It is {@link AbstractStreamTransformer_1_1} which unions all input streams and streams it
 * combination to the destination.
 *
 * @param <T> type of output data
 */
public final class StreamUnion<T> extends AbstractStreamTransformer_N_1<T> {
	// region creators
	private StreamUnion(Eventloop eventloop) {
		super(eventloop);
		this.outputProducer = new OutputProducer();
	}

	public static <T> StreamUnion<T> create(Eventloop eventloop) {return new StreamUnion<T>(eventloop);}
	// endregion

	protected final class InputConsumer extends AbstractInputConsumer<T> implements StreamDataReceiver<T> {
		private final OutputProducer outputProducer = (OutputProducer) StreamUnion.this.outputProducer;

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return outputProducer.getDownstreamDataReceiver() != null
					? outputProducer.getDownstreamDataReceiver()
					: this;
		}

		@Override
		public void onData(T item) {
			outputProducer.send(item);
		}

		@Override
		protected void onUpstreamEndOfStream() {
			if (allUpstreamsEndOfStream()) {
				outputProducer.sendEndOfStream();
			}
		}
	}

	public final class OutputProducer extends AbstractOutputProducer {

		@Override
		protected void onDownstreamStarted() {
			if (inputConsumers.isEmpty()) {
				sendEndOfStream();
			}
		}

		@Override
		protected void onDownstreamSuspended() {
			suspendAllUpstreams();
		}

		@Override
		protected void onDownstreamResumed() {
			resumeAllUpstreams();
		}

	}

	public StreamConsumer<T> newInput() {
		return addInput(new InputConsumer());
	}
}
