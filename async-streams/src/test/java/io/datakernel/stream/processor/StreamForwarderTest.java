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
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamForwarderTest {
	@Test
	public void test1() {
		NioEventloop eventloop = new NioEventloop();

		List<Integer> list = Arrays.asList(1, 2, 3);

		StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, list);
		StreamForwarder<Integer> forwarder = new StreamForwarder<>(eventloop);
		StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<>(eventloop, new ArrayList<Integer>());
		producer.streamTo(forwarder);
		forwarder.streamTo(consumer);

		eventloop.run();

		assertEquals(list, consumer.getList());
		assertTrue(((AbstractStreamProducer)producer).getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

	@Test
	public void test2() {
		final NioEventloop eventloop = new NioEventloop();

		List<Integer> list = Arrays.asList(1, 2, 3);

		StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, list);
		final StreamForwarder<Integer> forwarder = new StreamForwarder<>(eventloop);
		final StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<>(eventloop, new ArrayList<Integer>());
		producer.streamTo(forwarder);

		eventloop.postLater(new Runnable() {
			@Override
			public void run() {
				eventloop.postLater(new Runnable() {
					@Override
					public void run() {
						forwarder.streamTo(consumer);
					}
				});
			}
		});

		eventloop.run();

		assertEquals(list, consumer.getList());
		assertTrue(((AbstractStreamProducer)producer).getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

	@Test
	public void test3() {
		final NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> producer = StreamProducers.ofValue(eventloop, 1);
		final StreamForwarder<Integer> forwarder = new StreamForwarder<>(eventloop);
		final StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<>(eventloop, new ArrayList<Integer>());
		producer.streamTo(forwarder);

		eventloop.postLater(new Runnable() {
			@Override
			public void run() {
				forwarder.streamTo(consumer);
			}
		});

		eventloop.run();

		assertEquals(Arrays.asList(1), consumer.getList());
		assertTrue(((AbstractStreamProducer)producer).getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

	@Test
	public void test4() {
		final NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> producer = StreamProducers.closing(eventloop);
		final StreamForwarder<Integer> forwarder = new StreamForwarder<>(eventloop);
		final StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<>(eventloop, new ArrayList<Integer>());
		producer.streamTo(forwarder);

		eventloop.postLater(new Runnable() {
			@Override
			public void run() {
				forwarder.streamTo(consumer);
			}
		});

		eventloop.run();

		assertEquals(Arrays.asList(), consumer.getList());
		assertTrue(((AbstractStreamProducer)producer).getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

	@Test
	public void test5() {
		final NioEventloop eventloop = new NioEventloop();

		Exception e = new Exception();
		StreamProducer<Integer> producer = StreamProducers.closingWithError(eventloop, e);
		final StreamForwarder<Integer> forwarder = new StreamForwarder<>(eventloop);
		final StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<>(eventloop, new ArrayList<Integer>());
		producer.streamTo(forwarder);

		eventloop.postLater(new Runnable() {
			@Override
			public void run() {
				forwarder.streamTo(consumer);
			}
		});

		eventloop.run();

		assertTrue(((AbstractStreamProducer)consumer.getUpstream()).getStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
		assertEquals(e, consumer.getUpstream().getError());
//		assertTrue(((StreamProducerEx<?>) producer).getWiredConsumerStatus().isComplete());
	}

	@Test
	public void test6() {
		final NioEventloop eventloop = new NioEventloop();

		final StreamProducer<Integer> producer = StreamProducers.ofValue(eventloop, 1);
		final StreamForwarder<Integer> forwarder = new StreamForwarder<>(eventloop);
		final StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<>(eventloop, new ArrayList<Integer>());

		forwarder.streamTo(consumer);

		eventloop.postLater(new Runnable() {
			@Override
			public void run() {
				eventloop.postLater(new Runnable() {
					@Override
					public void run() {
						producer.streamTo(forwarder);
					}
				});
			}
		});

		eventloop.run();

		assertEquals(Arrays.asList(1), consumer.getList());
		assertTrue(((AbstractStreamProducer)producer).getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

}