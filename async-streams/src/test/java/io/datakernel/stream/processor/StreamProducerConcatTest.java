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

import static io.datakernel.stream.processor.Utils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamProducerConcatTest {

	@Test
	public void testSequence() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(4, 5, 6));

		AsyncIteratorWithSetter<StreamProducer<Integer>> producerSetter = AsyncIteratorWithSetter.createAsyncIteratorWithSetter(eventloop);

		StreamProducer<Integer> producer = new StreamProducers.StreamProducerConcat<>(eventloop, producerSetter);
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		producer.streamTo(consumer);

		eventloop.run();

		producerSetter.onNext(source1);
		eventloop.run();

		producerSetter.onNext(source2);
		eventloop.run();

		producerSetter.onEnd();
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6), consumer.getList());
		assertStatus(AbstractStreamProducer.StreamProducerStatus.END_OF_STREAM, consumer.getUpstream());
//		assertNull(source1.getWiredConsumerStatus());
//		assertNull(source2.getWiredConsumerStatus());
//		assertTrue(((AbstractStreamProducer)producer).getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

	@Test
	public void testSequenceException() throws Exception {
		NioEventloop eventloop = new NioEventloop();
		List<Integer> list = new ArrayList<>();

		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(4, 5, 6));

		AsyncIteratorWithSetter<StreamProducer<Integer>> producerSetter = AsyncIteratorWithSetter.createAsyncIteratorWithSetter(eventloop);

		StreamProducer<Integer> producer = new StreamProducers.StreamProducerConcat<>(eventloop, producerSetter);
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop, list);

		producer.streamTo(consumer);

		eventloop.run();

		producerSetter.onNext(source1);
		eventloop.run();

		producerSetter.onNext(source2);
		eventloop.run();

		producerSetter.onException(new Exception());
		eventloop.run();

		producerSetter.onNext(StreamProducers.ofIterable(eventloop, asList(1, 2, 3)));

		assertEquals(asList(1, 2, 3, 4, 5, 6), list);
//		assertTrue(((AbstractStreamProducer) consumer.getUpstream()).getStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
//		assertNull(source1.getWiredConsumerStatus());
//		assertNull(source2.getWiredConsumerStatus());
//		assertTrue(((AbstractStreamProducer)producer).getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

	@Test
	public void testConcat() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.ofIterable(eventloop, asList(4, 5, 6)),
				StreamProducers.<Integer>closing(eventloop));

		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toList(eventloop);

		producer.streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6), consumer.getList());
	}

	@Test
	public void testConcatException() {
		NioEventloop eventloop = new NioEventloop();
		List<Integer> list = new ArrayList<>();

		StreamProducer<Integer> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.ofIterable(eventloop, asList(4, 5, 6)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception()));

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListOneByOne(eventloop, list);

		producer.streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6), list);

	}

}