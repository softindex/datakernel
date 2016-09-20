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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

public final class StreamForwarderWithCounter extends AbstractStreamTransformer_1_1<ByteBuf, ByteBuf> {
	private InputConsumer inputConsumer;
	private OutputProducer outputProducer;
	private final long expectedSize;
	private long streamedSize = 0;

	private StreamForwarderWithCounter(Eventloop eventloop, long requiredSize) {
		super(eventloop);
		inputConsumer = new InputConsumer();
		outputProducer = new OutputProducer();
		expectedSize = requiredSize;
	}

	public static StreamForwarderWithCounter create(Eventloop eventloop, long requiredSize) {
		return new StreamForwarderWithCounter(eventloop, requiredSize);
	}

	private class InputConsumer extends AbstractInputConsumer implements StreamDataReceiver<ByteBuf> {
		@Override
		protected void onUpstreamEndOfStream() {
			if (expectedSize == streamedSize) {
				outputProducer.sendEndOfStream();
			} else {
				Exception e = new RemoteFsException("Expected and actual sizes mismatch. Expected: " + expectedSize + ", Actual: " + streamedSize);
				onError(e);
			}
		}

		@Override
		public StreamDataReceiver<ByteBuf> getDataReceiver() {
			return this;
		}

		@Override
		public void onData(ByteBuf item) {
			streamedSize += item.headRemaining();
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
