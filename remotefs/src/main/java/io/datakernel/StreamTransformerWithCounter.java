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

package io.datakernel;

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamStatus;

public class StreamTransformerWithCounter extends AbstractStreamTransformer_1_1<ByteBuf, ByteBuf> {
	private InputConsumer inputConsumer;
	private OutputProducer outputProducer;
	private final long expectedSize;
	private long streamedSize = 0;

	private ResultCallback<Long> positionCallback;

	public void setPositionCallback(ResultCallback<Long> positionCallback) {
		if (this.getOutput().getProducerStatus().isOpen()) {
			this.positionCallback = positionCallback;
		} else {
			if (this.getOutput().getProducerStatus() == StreamStatus.END_OF_STREAM) {
				positionCallback.onResult(streamedSize);
			} else {
				positionCallback.onException(this.getOutput().getProducerException());
			}
		}
	}

	public StreamTransformerWithCounter(Eventloop eventloop, long requiredSize) {
		super(eventloop);
		inputConsumer = new InputConsumer();
		outputProducer = new OutputProducer();
		expectedSize = requiredSize;
	}

	private class InputConsumer extends AbstractInputConsumer implements StreamDataReceiver<ByteBuf> {
		@Override
		protected void onUpstreamEndOfStream() {
			if (expectedSize == streamedSize) {
				outputProducer.sendEndOfStream();
				if (positionCallback != null) {
					positionCallback.onResult(streamedSize);
				}
			} else {
				Exception e = new Exception("Expected and actual sizes mismatch. Expected: " + expectedSize + ", Actual: " + streamedSize);
				onError(e);
				if (positionCallback != null) {
					positionCallback.onException(e);
				}
			}
		}

		@Override
		public StreamDataReceiver<ByteBuf> getDataReceiver() {
			return this;
		}

		@Override
		public void onData(ByteBuf item) {
			streamedSize += (item.limit() - item.position());
			outputProducer.send(item);
		}
	}

	private class OutputProducer extends AbstractOutputProducer {
		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			inputConsumer.resume();
		}
	}
}
