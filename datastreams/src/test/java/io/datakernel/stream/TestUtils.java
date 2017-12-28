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
import io.datakernel.stream.processor.StreamTransformer;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestUtils {

	public static StreamStatus[] producerStatuses(List<? extends StreamProducer<?>> streamProducers) {
		StreamStatus[] result = new StreamStatus[streamProducers.size()];
		for (int i = 0; i < streamProducers.size(); i++) {
			StreamProducer<?> streamProducer = streamProducers.get(i);
			result[i] = ((AbstractStreamProducer<?>) streamProducer).getStatus();
		}
		return result;
	}

	public static StreamStatus[] consumerStatuses(List<? extends StreamConsumer<?>> streamConsumers) {
		StreamStatus[] result = new StreamStatus[streamConsumers.size()];
		for (int i = 0; i < streamConsumers.size(); i++) {
			StreamConsumer<?> streamConsumer = streamConsumers.get(i);
			result[i] = ((AbstractStreamConsumer<?>) streamConsumer).getStatus();
		}
		return result;
	}

	public static void assertConsumerStatuses(StreamStatus expected, List<? extends StreamConsumer<?>> streamConsumers) {
		StreamStatus[] statuses = consumerStatuses(streamConsumers);
		for (StreamStatus status : statuses) {
			assertEquals("Expected " + expected + ", actual: " + Arrays.toString(statuses), expected, status);
		}
	}

	public static void assertProducerStatuses(StreamStatus expected, List<? extends StreamProducer<?>> streamProducers) {
		StreamStatus[] statuses = producerStatuses(streamProducers);
		for (StreamStatus status : statuses) {
			assertEquals("Expected " + expected + ", actual: " + Arrays.toString(statuses), expected, status);
		}
	}

	public static void assertStatus(StreamStatus expectedStatus, StreamProducer<?> streamProducer) {
		if (streamProducer instanceof StreamProducerDecorator) {
			assertStatus(expectedStatus, ((StreamProducerDecorator) streamProducer).getActualProducer());
			return;
		}
		if (expectedStatus == StreamStatus.END_OF_STREAM && streamProducer instanceof StreamProducers.EndOfStreamImpl)
			return;
		if (expectedStatus == StreamStatus.CLOSED_WITH_ERROR && streamProducer instanceof StreamProducers.ClosingWithErrorImpl)
			return;
		assertEquals(expectedStatus, ((AbstractStreamProducer<?>) streamProducer).getStatus());
	}

	public static void assertStatus(StreamStatus expectedStatus, StreamConsumer<?> streamConsumer) {
		if (streamConsumer instanceof StreamConsumerDecorator) {
			assertStatus(expectedStatus, ((StreamConsumerDecorator) streamConsumer).getActualConsumer());
			return;
		}
		if (expectedStatus == StreamStatus.CLOSED_WITH_ERROR && streamConsumer instanceof StreamConsumers.ClosingWithErrorImpl)
			return;
		assertEquals(expectedStatus, ((AbstractStreamConsumer<?>) streamConsumer).getStatus());
	}

	public static class CountTransformer<T> implements StreamTransformer<T, T> {
		private final AbstractStreamConsumer<T> input;
		private final AbstractStreamProducer<T> output;

		private boolean isEndOfStream = false;
		private int suspended = 0;
		private int resumed = 0;

		public CountTransformer(Eventloop eventloop) {
			this.input = new Input(eventloop);
			this.output = new Output(eventloop);
		}

		@Override
		public StreamConsumer<T> getInput() {
			return input;
		}

		@Override
		public StreamProducer<T> getOutput() {
			return output;
		}

		public boolean isEndOfStream() {
			return isEndOfStream;
		}

		public int getSuspended() {
			return suspended;
		}

		public int getResumed() {
			return resumed;
		}

		protected final class Input extends AbstractStreamConsumer<T> {
			protected Input(Eventloop eventloop) {
				super(eventloop);
			}

			@Override
			protected void onEndOfStream() {
				isEndOfStream = true;
				output.sendEndOfStream();
			}

			@Override
			protected void onError(Throwable t) {
				output.closeWithError(t);
			}

		}

		protected final class Output extends AbstractStreamProducer<T> {
			protected Output(Eventloop eventloop) {
				super(eventloop);
			}

			@Override
			protected void onSuspended() {
				suspended++;
				input.getProducer().suspend();
			}

			@Override
			protected void onError(Throwable t) {
				input.closeWithError(t);
			}

			@Override
			protected void onProduce(StreamDataReceiver<T> dataReceiver) {
				resumed++;
				input.getProducer().produce(dataReceiver);
			}
		}
	}

}
