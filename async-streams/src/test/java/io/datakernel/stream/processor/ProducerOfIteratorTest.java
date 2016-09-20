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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ProducerOfIteratorTest {

	@Test
	public void test1() {
		Eventloop eventloop = Eventloop.create();

		List<Integer> list = Arrays.asList(1, 2, 3);

		StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, list);
		StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<>(eventloop, new ArrayList<Integer>());
		producer.streamTo(consumer);

		eventloop.run();

		assertEquals(list, consumer.getList());
		assertEquals(END_OF_STREAM, producer.getProducerStatus());
		assertEquals(END_OF_STREAM, consumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

}


