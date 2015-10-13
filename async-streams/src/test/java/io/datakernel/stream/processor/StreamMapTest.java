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

import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.stream.StreamStatus.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamMapTest {

	private static final StreamMap.MapperProjection<Integer, Integer> FUNCTION = new StreamMap.MapperProjection<Integer, Integer>() {
		@Override
		protected Integer apply(Integer input) {
			return input + 10;
		}
	};

	@Test
	public void test1() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamMap<Integer, Integer> projection = new StreamMap<>(eventloop, FUNCTION);
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(projection);
		projection.streamTo(consumer);

		eventloop.run();
		assertEquals(asList(11, 12, 13), consumer.getList());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, projection.getConsumerStatus());
		assertEquals(END_OF_STREAM, projection.getProducerStatus());
	}

	@Test
	public void testWithError() throws Exception {
		NioEventloop eventloop = new NioEventloop();
		List<Integer> list = new ArrayList<>();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamMap<Integer, Integer> projection = new StreamMap<>(eventloop, FUNCTION);

		TestStreamConsumers.TestConsumerToList<Integer> consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (item == 12) {
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

		source.streamTo(projection);
		projection.streamTo(consumer);

		eventloop.run();
		assertTrue(list.size() == 2);
		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumer.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, projection.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, projection.getProducerStatus());
	}

//	@Test
//	public void testEndofStream() throws Exception {
//		NioEventloop eventloop = new NioEventloop();
//		List<Integer> list = new ArrayList<>();
//
//		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
//		StreamMap<Integer, Integer> projection = new StreamMap<>(eventloop, FUNCTION);
//
//		StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<Integer>(eventloop, list) {
//			@Override
//			public void onData(Integer item) {
//				super.onData(item);
//				if (item == 12) {
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
//		source.streamTo(projection);
//		projection.streamTo(consumer);
//
//		eventloop.run();
//
//		assertTrue(list.size() == 2);
//		assertTrue(((AbstractStreamProducer)source).getStatus() == AbstractStreamProducer.END_OF_STREAM);
//	}

	@Test
	public void testProducerWithError() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, 1),
				StreamProducers.ofValue(eventloop, 2),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception()));

		StreamMap<Integer, Integer> projection = new StreamMap<>(eventloop, FUNCTION);

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListOneByOne(eventloop, list);

		source.streamTo(projection);
		projection.streamTo(consumer);

		eventloop.run();
		assertTrue(list.size() == 2);
		assertEquals(CLOSED_WITH_ERROR, consumer.getUpstream().getProducerStatus());
	}

}
