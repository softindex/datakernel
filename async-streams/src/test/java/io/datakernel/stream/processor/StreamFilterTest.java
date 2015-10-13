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

import com.google.common.base.Predicate;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.stream.StreamStatus.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamFilterTest {
	@Test
	public void test1() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));

		Predicate<Integer> predicate = new Predicate<Integer>() {
			@Override
			public boolean apply(Integer input) {
				return input % 2 == 1;
			}
		};
		StreamFilter<Integer> filter = new StreamFilter<>(eventloop, predicate);

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(filter);
		filter.streamTo(consumer);

		eventloop.run();
		assertEquals(asList(1, 3), consumer.getList());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, filter.getConsumerStatus());
		assertEquals(END_OF_STREAM, filter.getProducerStatus());
	}

	@Test
	public void testWithError() {
		NioEventloop eventloop = new NioEventloop();
		List<Integer> list = new ArrayList<>();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5));

		Predicate<Integer> predicate = new Predicate<Integer>() {
			@Override
			public boolean apply(Integer input) {
				return input % 2 != 2;
			}
		};
		StreamFilter<Integer> streamFilter = new StreamFilter<>(eventloop, predicate);

		TestStreamConsumers.TestConsumerToList<Integer> consumer1 = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (item == 3) {
					closeWithError(new Exception());
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

		source.streamTo(streamFilter);
		streamFilter.streamTo(consumer1);

		eventloop.run();

		assertEquals(asList(1, 2, 3), list);
		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumer1.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, streamFilter.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, streamFilter.getProducerStatus());

	}

//	@Test
//	public void testEndOfStream() {
//		NioEventloop eventloop = new NioEventloop();
//		List<Integer> list = new ArrayList<>();
//
//		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5));
//
//		Predicate<Integer> predicate = new Predicate<Integer>() {
//			@Override
//			public boolean apply(Integer input) {
//				return input % 2 != 2;
//			}
//		};
//		StreamFilter<Integer> streamFilter = new StreamFilter<>(eventloop, predicate);
//
//		TestStreamConsumers.TestConsumerToList<Integer> consumer1 = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
//			@Override
//			public void onData(Integer item) {
//				list.add(item);
//				if (item == 3) {
//					onProducerEndOfStream();
//					return;
//				}
//				upstreamProducer.onConsumerSuspended();
//				eventloop.post(new Runnable() {
//					@Override
//					public void run() {
//						upstreamProducer.onConsumerResumed();
//					}
//				});
//			}
//		};
//
//		source.streamTo(streamFilter);
//		streamFilter.streamTo(consumer1);
//
//		eventloop.run();
//
//		assertEquals(asList(1, 2, 3), list);
//		assertTrue(((AbstractStreamProducer)source).getStatus() == AbstractStreamProducer.END_OF_STREAM);
//
//	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProducerDisconnectWithError() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, Arrays.asList(1, 2, 3)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception()));

		Predicate<Integer> predicate = new Predicate<Integer>() {
			@Override
			public boolean apply(Integer input) {
				return input % 2 != 2;
			}
		};
		StreamFilter<Integer> streamFilter = new StreamFilter<>(eventloop, predicate);

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList consumer = TestStreamConsumers.toListOneByOne(eventloop, list);

		source.streamTo(streamFilter);
		streamFilter.streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 3);
		assertEquals(CLOSED_WITH_ERROR, consumer.getUpstream().getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumer.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, streamFilter.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, streamFilter.getProducerStatus());
	}
}
