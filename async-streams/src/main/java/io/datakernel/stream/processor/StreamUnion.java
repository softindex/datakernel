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
import io.datakernel.stream.*;

/**
 * It is {@link AbstractStreamTransformer_1_1} which unions all input streams and streams it
 * combination to the destination.
 *
 * @param <T> type of output data
 */
public final class StreamUnion<T> extends AbstractStreamTransformer_M_1<T> implements StreamDataReceiver<T> {

	public StreamUnion(Eventloop eventloop) {
		super(eventloop);
	}

	private class InputImpl extends AbstractStreamConsumer<T> {
		protected InputImpl(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return StreamUnion.this.downstreamDataReceiver != null ? StreamUnion.this.downstreamDataReceiver : StreamUnion.this;
		}

		@Override
		public void onProducerEndOfStream() {
			if (allUpstreamsEndOfStream()) {
				sendEndOfStream();
			}
		}

		@Override
		public void onProducerError(Exception e) {
			upstreamProducer.onConsumerError(e);
			onConsumerError(e);
		}
	}

	/**
	 * This method is called if consumer was changed for changing consumer status and checks if input
	 * streams are at out.
	 */
	@Override
	protected void onProducerStarted() {
		for (StreamConsumer<?> input : inputs) {
			input.getUpstream().bindDataReceiver();
		}
		if (inputs.isEmpty()) {
			sendEndOfStream();
		}
	}

	@Override
	protected void onSuspended() {
		suspendAllUpstreams();
	}

	@Override
	public void onResumed() {
		resumeAllUpstreams();
	}

	/**
	 * Adds the new input stream to this StreamUnion
	 *
	 * @return the new stream
	 */
	public StreamConsumer<T> newInput() {
		return addInput(new InputImpl(eventloop));
	}

	@Override
	public void onData(T item) {
		downstreamDataReceiver.onData(item);
	}
}
