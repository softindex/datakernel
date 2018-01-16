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
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamProducer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.DataStreams.stream;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.TestUtils.assertStatus;
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
	public void test1() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamProducer<Integer> source = StreamProducer.of(1, 2, 3);
		StreamMap<Integer, Integer> projection = StreamMap.create(FUNCTION);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.randomlySuspending();

		stream(source, projection.getInput());
		stream(projection.getOutput(), consumer);

		eventloop.run();
		assertEquals(asList(11, 12, 13), consumer.getList());
		assertStatus(END_OF_STREAM, source);
		assertStatus(END_OF_STREAM, projection.getInput());
		assertStatus(END_OF_STREAM, projection.getOutput());
	}

	@Test
	public void testWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		List<Integer> list = new ArrayList<>();

		StreamProducer<Integer> source = StreamProducer.of(1, 2, 3);
		StreamMap<Integer, Integer> projection = StreamMap.create(FUNCTION);

		StreamConsumerToList<Integer> consumer = new StreamConsumerToList<Integer>(list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (item == 12) {
					closeWithError(new Exception("Test Exception"));
					return;
				}
				getProducer().suspend();
				eventloop.post(() -> getProducer().produce(this));
			}
		};

		stream(source, projection.getInput());
		stream(projection.getOutput(), consumer);

		eventloop.run();
		assertTrue(list.size() == 2);
		assertStatus(CLOSED_WITH_ERROR, source);
		assertStatus(CLOSED_WITH_ERROR, consumer);
		assertStatus(CLOSED_WITH_ERROR, projection.getInput());
		assertStatus(CLOSED_WITH_ERROR, projection.getOutput());
	}

	@Test
	public void testProducerWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamProducer<Integer> source = StreamProducer.concat(
				StreamProducer.of(1),
				StreamProducer.of(2),
				StreamProducer.closingWithError(new Exception("Test Exception")));

		StreamMap<Integer, Integer> projection = StreamMap.create(FUNCTION);

		List<Integer> list = new ArrayList<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.oneByOne(list);

		stream(source, projection.getInput());
		stream(projection.getOutput(), consumer);

		eventloop.run();
		assertTrue(list.size() == 2);
		assertStatus(CLOSED_WITH_ERROR, consumer.getProducer());
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamProducer<Integer> source = StreamProducer.of(1, 2, 3);
		StreamMap<Integer, Integer> projection = StreamMap.create(FUNCTION);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.randomlySuspending();

		stream(source, projection.getInput());
		eventloop.run();

		stream(projection.getOutput(), consumer);
		eventloop.run();

		assertEquals(asList(11, 12, 13), consumer.getList());
		assertStatus(END_OF_STREAM, source);
		assertStatus(END_OF_STREAM, projection.getInput());
		assertStatus(END_OF_STREAM, projection.getOutput());
	}
}
