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
import io.datakernel.stream.AbstractStreamProducer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

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
		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toListRandomlySuspending(eventloop);

		source1.streamTo(streamFunction);
		streamFunction.streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4, 9), consumer.getList());

		assertTrue(((AbstractStreamProducer)source1).getStatus() == AbstractStreamProducer.END_OF_STREAM);
		assertTrue(streamFunction.getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

}
