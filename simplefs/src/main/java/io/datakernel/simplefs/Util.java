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

package io.datakernel.simplefs;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

class Util {
	static class CounterTransformer extends AbstractStreamTransformer_1_1<ByteBuf, ByteBuf> {
		private InputConsumer inputConsumer;
		private OutputProducer outputProducer;
		private long expectedSize;

		protected CounterTransformer(Eventloop eventloop, long requiredSize) {
			super(eventloop);
			inputConsumer = new InputConsumer();
			outputProducer = new OutputProducer();
			expectedSize = requiredSize;
		}

		private class InputConsumer extends AbstractInputConsumer implements StreamDataReceiver<ByteBuf> {
			@Override
			protected void onUpstreamEndOfStream() {
				if (expectedSize == 0) {
					outputProducer.sendEndOfStream();
				} else {
					onError(new Exception("Expected and actual sizes mismatch"));
				}
			}

			@Override
			public StreamDataReceiver<ByteBuf> getDataReceiver() {
				return this;
			}

			@Override
			public void onData(ByteBuf item) {
				expectedSize -= (item.limit() - item.position());
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
}