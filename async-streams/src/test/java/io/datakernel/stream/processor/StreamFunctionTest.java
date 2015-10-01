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

import com.google.common.base.Function;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.stream.StreamProducers.concat;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamFunctionTest {

	@Test
	public void testFunction() {
		NioEventloop eventloop = new NioEventloop();

		StreamFunction<Integer, Integer> streamFunction = new StreamFunction<>(eventloop, new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer input) {
				return input * input;
			}
		});

		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source1.streamTo(streamFunction);
		streamFunction.streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4, 9), consumer.getList());

		assertTrue(((AbstractStreamProducer)source1).getStatus() == AbstractStreamProducer.END_OF_STREAM);
		assertTrue(streamFunction.upstreamConsumerStatus() == AbstractStreamConsumer.CLOSED);
		assertTrue(streamFunction.downstreamProducerStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

	@Test
	public void testFunctionConsumerError() {
		NioEventloop eventloop = new NioEventloop();

		StreamFunction<Integer, Integer> streamFunction = new StreamFunction<>(eventloop, new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer input) {
				return input * input;
			}
		});

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		TestStreamConsumers.TestConsumerToList<Integer> consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (list.size() == 2) {
					closeWithError(new Exception());
					return;
				}
				suspend();
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						resume();
					}
				});
			}
		};

		source1.streamTo(streamFunction);
		streamFunction.streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4), list);

		assertTrue(((AbstractStreamProducer)source1).getStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
		assertTrue(streamFunction.upstreamConsumerStatus() == AbstractStreamConsumer.CLOSED_WITH_ERROR);
		assertTrue(streamFunction.downstreamProducerStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
	}

	@Test
	public void testFunctionProducerError() {
		NioEventloop eventloop = new NioEventloop();

		StreamFunction<Integer, Integer> streamFunction = new StreamFunction<>(eventloop, new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer input) {
				return input * input;
			}
		});

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> source1 = concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.ofIterable(eventloop, asList(4, 5, 6)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception()),
				StreamProducers.ofIterable(eventloop, asList(4, 5, 6)));

		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toList(eventloop, list);

		source1.streamTo(streamFunction);
		streamFunction.streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4, 9, 16, 25, 36), list);

		assertTrue(((AbstractStreamProducer)consumer.getUpstream()).getStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
		assertTrue(streamFunction.upstreamConsumerStatus() == AbstractStreamConsumer.CLOSED_WITH_ERROR);
		assertTrue(streamFunction.downstreamProducerStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
	}

}
