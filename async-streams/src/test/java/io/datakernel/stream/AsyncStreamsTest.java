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

import io.datakernel.async.AsyncGetterWithSetter;
import io.datakernel.eventloop.NioEventloop;
import org.junit.Test;

import static io.datakernel.async.AsyncCallbacks.createAsyncGetterWithSetter;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class AsyncStreamsTest {
	@Test
	public void testDelayedProducer() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));

		AsyncGetterWithSetter<StreamProducer<Integer>> producerSetter = createAsyncGetterWithSetter(eventloop);

		StreamProducer<Integer> producer = StreamProducers.asynchronouslyResolving(eventloop, producerSetter);
		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toListRandomlySuspending(eventloop);

		producer.streamTo(consumer);

		eventloop.run();
		assertFalse(consumer.getUpstreamStatus() == AbstractStreamProducer.CLOSED);

		producerSetter.onResult(source);
		eventloop.run();
		assertEquals(asList(1, 2, 3), consumer.getList());
		assertTrue(consumer.getUpstreamStatus() == AbstractStreamProducer.END_OF_STREAM);
		assertTrue(((AbstractStreamProducer)source).getStatus() == AbstractStreamProducer.END_OF_STREAM);
////		assertNull(producer.getWiredConsumerStatus());
	}

}