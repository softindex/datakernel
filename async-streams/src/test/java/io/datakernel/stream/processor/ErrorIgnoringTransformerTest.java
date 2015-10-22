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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ErrorIgnoringTransformerTest {

	@Test
	public void test1() {
		NioEventloop eventloop = new NioEventloop();
		StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));

		ErrorIgnoringTransformer<Integer> errorIgnoringTransformer = new ErrorIgnoringTransformer<>(eventloop);

		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toList(eventloop);

		producer.streamTo(errorIgnoringTransformer);
		errorIgnoringTransformer.streamTo(consumer);

		eventloop.run();

		assertEquals(consumer.getList(), asList(1, 2, 3));
		assertEquals(consumer.getConsumerStatus(), StreamStatus.END_OF_STREAM);
		assertEquals(producer.getProducerStatus(), StreamStatus.END_OF_STREAM);
		assertEquals(errorIgnoringTransformer.getConsumerStatus(), StreamStatus.END_OF_STREAM);
		assertEquals(errorIgnoringTransformer.getProducerStatus(), StreamStatus.END_OF_STREAM);
	}

	@Test
	public void testProducerWithError() {
		NioEventloop eventloop = new NioEventloop();
		StreamProducer<Integer> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception("Test Exception")));

		ErrorIgnoringTransformer<Integer> errorIgnoringTransformer = new ErrorIgnoringTransformer<>(eventloop);

		List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toList(eventloop, list);

		producer.streamTo(errorIgnoringTransformer);
		errorIgnoringTransformer.streamTo(consumer);

		eventloop.run();

		assertEquals(list, asList(1, 2, 3));
		assertEquals(consumer.getConsumerStatus(), StreamStatus.END_OF_STREAM);
		assertEquals(producer.getProducerStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(errorIgnoringTransformer.getConsumerStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(errorIgnoringTransformer.getProducerStatus(), StreamStatus.END_OF_STREAM);
	}

	@Test
	public void testConsumerWithError() {
		NioEventloop eventloop = new NioEventloop();
		StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5, 6));

		ErrorIgnoringTransformer<Integer> errorIgnoringTransformer = new ErrorIgnoringTransformer<>(eventloop);

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				super.onData(item);
				if (list.size() >= 2) {
					closeWithError(new Exception("Test Exception"));
				}
			}
		};

		producer.streamTo(errorIgnoringTransformer);
		errorIgnoringTransformer.streamTo(consumer);

		eventloop.run();

		assertEquals(list, asList(1, 2));
		assertEquals(consumer.getConsumerStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(producer.getProducerStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(errorIgnoringTransformer.getConsumerStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(errorIgnoringTransformer.getProducerStatus(), StreamStatus.CLOSED_WITH_ERROR);
	}
}
