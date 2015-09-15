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

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class StreamProducerConcatTest {

	@Test
	public void testSequence() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(4, 5, 6));

		AsyncIteratorWithSetter<StreamProducer<Integer>> producerSetter = AsyncIteratorWithSetter.createAsyncIteratorWithSetter(eventloop);

		StreamProducer<Integer> producer = new StreamProducers.StreamProducerConcat<>(eventloop, producerSetter);
		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toListRandomlySuspending(eventloop);

		producer.streamTo(consumer);

		eventloop.run();
		assertFalse(consumer.getUpstreamStatus() == AbstractStreamProducer.CLOSED);

		producerSetter.onNext(source1);
		eventloop.run();

		producerSetter.onNext(source2);
		eventloop.run();

		producerSetter.onEnd();
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6), consumer.getList());
		assertTrue(consumer.getUpstreamStatus() == AbstractStreamProducer.CLOSED);
//		assertNull(source1.getWiredConsumerStatus());
//		assertNull(source2.getWiredConsumerStatus());
		assertTrue(((AbstractStreamProducer)producer).getStatus() == AbstractStreamProducer.CLOSED);
	}

}