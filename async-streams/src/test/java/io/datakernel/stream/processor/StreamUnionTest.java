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

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.processor.Utils.assertConsumerStatuses;
import static io.datakernel.stream.processor.Utils.consumerStatuses;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class StreamUnionTest {
	@Test
	public void test1() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamUnion<Integer> streamUnion = StreamUnion.create(eventloop);

		StreamProducer<Integer> source0 = StreamProducers.closing(eventloop);
		StreamProducer<Integer> source1 = StreamProducers.ofValue(eventloop, 1);
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(2, 3));
		StreamProducer<Integer> source3 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);
		StreamProducer<Integer> source4 = StreamProducers.ofIterable(eventloop, asList(4, 5));
		StreamProducer<Integer> source5 = StreamProducers.ofIterable(eventloop, asList(6));
		StreamProducer<Integer> source6 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source0.streamTo(streamUnion.newInput());
		source1.streamTo(streamUnion.newInput());
		source2.streamTo(streamUnion.newInput());
		source3.streamTo(streamUnion.newInput());
		source4.streamTo(streamUnion.newInput());
		source5.streamTo(streamUnion.newInput());
		source6.streamTo(streamUnion.newInput());
		streamUnion.getOutput().streamTo(consumer);
		eventloop.run();

		List<Integer> result = consumer.getList();
		Collections.sort(result);
		assertEquals(asList(1, 2, 3, 4, 5, 6), result);

		assertEquals(END_OF_STREAM, source0.getProducerStatus());
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(END_OF_STREAM, source3.getProducerStatus());
		assertEquals(END_OF_STREAM, source4.getProducerStatus());
		assertEquals(END_OF_STREAM, source5.getProducerStatus());
		assertEquals(END_OF_STREAM, source6.getProducerStatus());

		assertEquals(END_OF_STREAM, streamUnion.getOutput().getProducerStatus());
		assertConsumerStatuses(END_OF_STREAM, streamUnion.getInputs());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithError() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamUnion<Integer> streamUnion = StreamUnion.create(eventloop);

		StreamProducer<Integer> source0 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(4, 5));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(6, 7));

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (item == 1) {
					closeWithError(new Exception("Test Exception"));
					return;
				}
				upstreamProducer.onConsumerSuspended();
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						upstreamProducer.onConsumerResumed();
					}
				});
			}
		};

		source0.streamTo(streamUnion.newInput());
		source1.streamTo(streamUnion.newInput());
		source2.streamTo(streamUnion.newInput());

		streamUnion.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(Arrays.asList(6, 7, 4, 5, 1), list);
		assertEquals(CLOSED_WITH_ERROR, source0.getProducerStatus());
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());

		assertEquals(CLOSED_WITH_ERROR, streamUnion.getOutput().getProducerStatus());
		assertArrayEquals(new StreamStatus[]{CLOSED_WITH_ERROR, END_OF_STREAM, END_OF_STREAM},
				consumerStatuses(streamUnion.getInputs()));
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testProducerWithError() {
		Eventloop eventloop = Eventloop.create();

		StreamUnion<Integer> streamUnion = StreamUnion.create(eventloop);

		StreamProducer<Integer> source0 = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, Arrays.asList(1, 2)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception("Test Exception"))
		);
		StreamProducer<Integer> source1 = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, Arrays.asList(7, 8, 9)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception("Test Exception"))
		);

		List<Integer> list = new ArrayList<>();
		StreamConsumer<Integer> consumer = TestStreamConsumers.toListOneByOne(eventloop, list);

		source0.streamTo(streamUnion.newInput());
		source1.streamTo(streamUnion.newInput());

		streamUnion.getOutput().streamTo(consumer);
		eventloop.run();

		assertTrue(list.size() == 3);
		assertEquals(CLOSED_WITH_ERROR, streamUnion.getOutput().getProducerStatus());
		assertConsumerStatuses(CLOSED_WITH_ERROR, streamUnion.getInputs());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create();

		StreamUnion<Integer> streamUnion = StreamUnion.create(eventloop);

		StreamProducer<Integer> source0 = StreamProducers.closing(eventloop);
		StreamProducer<Integer> source1 = StreamProducers.ofValue(eventloop, 1);
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(2, 3));
		StreamProducer<Integer> source3 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);
		StreamProducer<Integer> source4 = StreamProducers.ofIterable(eventloop, asList(4, 5));
		StreamProducer<Integer> source5 = StreamProducers.ofIterable(eventloop, asList(6));
		StreamProducer<Integer> source6 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source0.streamTo(streamUnion.newInput());
		source1.streamTo(streamUnion.newInput());
		source2.streamTo(streamUnion.newInput());
		source3.streamTo(streamUnion.newInput());
		source4.streamTo(streamUnion.newInput());
		source5.streamTo(streamUnion.newInput());
		source6.streamTo(streamUnion.newInput());
		eventloop.run();

		streamUnion.getOutput().streamTo(consumer);
		eventloop.run();

		List<Integer> result = consumer.getList();
		Collections.sort(result);
		assertEquals(asList(1, 2, 3, 4, 5, 6), result);

		assertEquals(END_OF_STREAM, source0.getProducerStatus());
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(END_OF_STREAM, source3.getProducerStatus());
		assertEquals(END_OF_STREAM, source4.getProducerStatus());
		assertEquals(END_OF_STREAM, source5.getProducerStatus());
		assertEquals(END_OF_STREAM, source6.getProducerStatus());

		assertEquals(END_OF_STREAM, streamUnion.getOutput().getProducerStatus());
		assertConsumerStatuses(END_OF_STREAM, streamUnion.getInputs());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithoutProducer() {
		Eventloop eventloop = Eventloop.create();

		StreamUnion<Integer> streamUnion = StreamUnion.create(eventloop);
		CheckCallCallback checkCallCallback = new CheckCallCallback();
		StreamConsumers.ToList<Integer> toList = StreamConsumers.toList(eventloop);
		toList.setCompletionCallback(checkCallCallback);

		streamUnion.getOutput().streamTo(toList);
		eventloop.run();

		assertTrue(checkCallCallback.isCall());
		assertThat(eventloop, doesntHaveFatals());
	}

	class CheckCallCallback implements CompletionCallback {
		private int onComplete;
		private int onException;

		@Override
		public void onComplete() {
			onComplete++;
		}

		@Override
		public void onException(Exception exception) {
			onException++;
		}

		public int getOnComplete() {
			return onComplete;
		}

		public int getOnException() {
			return onException;
		}

		public boolean isCall() {
			return (onComplete == 1 && onException == 0) || (onComplete == 0 && onException == 1);
		}
	}
}
