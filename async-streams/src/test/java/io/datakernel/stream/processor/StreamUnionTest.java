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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class StreamUnionTest {
	@Test
	public void test1() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamUnion<Integer> streamUnion = new StreamUnion<>(eventloop);

		StreamProducer<Integer> source0 = StreamProducers.closing(eventloop);
		StreamProducer<Integer> source1 = StreamProducers.ofValue(eventloop, 1);
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(2, 3));
		StreamProducer<Integer> source3 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);
		StreamProducer<Integer> source4 = StreamProducers.ofIterable(eventloop, asList(4, 5));
		StreamProducer<Integer> source5 = StreamProducers.ofIterable(eventloop, asList(6));
		StreamProducer<Integer> source6 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);

		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toListRandomlySuspending(eventloop);

		source0.streamTo(streamUnion.newInput());
		source1.streamTo(streamUnion.newInput());
		source2.streamTo(streamUnion.newInput());
		source3.streamTo(streamUnion.newInput());
		source4.streamTo(streamUnion.newInput());
		source5.streamTo(streamUnion.newInput());
		source6.streamTo(streamUnion.newInput());
		streamUnion.streamTo(consumer);
		eventloop.run();

		List<Integer> result = consumer.getList();
		Collections.sort(result);
		assertEquals(asList(1, 2, 3, 4, 5, 6), result);

		assertTrue(((AbstractStreamProducer)source0).getStatus() == AbstractStreamProducer.CLOSED);
		assertTrue(((AbstractStreamProducer)source1).getStatus() == AbstractStreamProducer.CLOSED);
		assertTrue(((AbstractStreamProducer)source2).getStatus() == AbstractStreamProducer.CLOSED);
		assertTrue(((AbstractStreamProducer)source3).getStatus() == AbstractStreamProducer.CLOSED);
		assertTrue(((AbstractStreamProducer)source4).getStatus() == AbstractStreamProducer.CLOSED);
		assertTrue(((AbstractStreamProducer)source5).getStatus() == AbstractStreamProducer.CLOSED);
		assertTrue(((AbstractStreamProducer)source6).getStatus() == AbstractStreamProducer.CLOSED);
	}

	@Test
	public void testWithError() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamUnion<Integer> streamUnion = new StreamUnion<>(eventloop);

		StreamProducer<Integer> source0 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(4, 5));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(6, 7));

		List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				super.onData(item);
				if (item == 5) {
					closeUpstreamWithError(new Exception());
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

		streamUnion.streamTo(consumer);
		eventloop.run();

		assertTrue(list.size() == 4);
		assertTrue(((AbstractStreamProducer)source0).getStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
		assertTrue(((AbstractStreamProducer)source1).getStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
//		source2.getStatus() should be equals to CLOSE
//		assertTrue(source2.getStatus() == CLOSED_WITH_ERROR);
	}

	@Test
	public void testEndOfStream() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamUnion<Integer> streamUnion = new StreamUnion<>(eventloop);

		StreamProducer<Integer> source0 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(4, 5));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(6, 7));

		List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				super.onData(item);
				if (item == 5) {
					onProducerEndOfStream();
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

		streamUnion.streamTo(consumer);
		eventloop.run();

		assertTrue(list.size() == 4);
		assertTrue(((AbstractStreamProducer)source0).getStatus() == AbstractStreamProducer.CLOSED);
		assertTrue(((AbstractStreamProducer)source1).getStatus() == AbstractStreamProducer.CLOSED);
		assertTrue(((AbstractStreamProducer)source2).getStatus() == AbstractStreamProducer.CLOSED);

	}

	@Test
	public void testProducerWithError() {
		NioEventloop eventloop = new NioEventloop();

		StreamUnion<Integer> streamUnion = new StreamUnion<>(eventloop);

		StreamProducer<Integer> source0 = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, Arrays.asList(1, 2)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception()),
				StreamProducers.ofValue(eventloop, 3)
		);

		StreamProducer<Integer> source1 = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, Arrays.asList(7, 8, 9)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception())
		);

		List<Integer> list = new ArrayList<>();
		StreamConsumer<Integer> consumer = StreamConsumers.toListOneByOne(eventloop, list);

		source0.streamTo(streamUnion.newInput());
		source1.streamTo(streamUnion.newInput());

		streamUnion.streamTo(consumer);
		eventloop.run();

		assertTrue(list.size() == 3);

		assertTrue(((AbstractStreamProducer)source0).getStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
		assertTrue(((AbstractStreamProducer)source1).getStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);

	}
}
