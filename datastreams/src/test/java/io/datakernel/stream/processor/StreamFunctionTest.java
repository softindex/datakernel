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
import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamProducer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.DataStreams.stream;
import static io.datakernel.stream.StreamConsumers.decorator;
import static io.datakernel.stream.StreamConsumers.randomlySuspending;
import static io.datakernel.stream.StreamProducer.concat;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.TestUtils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamFunctionTest {

	@Test
	public void testFunction() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamProducer<Integer> source1 = StreamProducer.of(1, 2, 3);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		stream(source1.with(StreamFunction.create(input -> input * input)), consumer.with(randomlySuspending()));
		eventloop.run();

		assertEquals(asList(1, 4, 9), consumer.getList());

		assertStatus(END_OF_STREAM, source1);
//		assertStatus(END_OF_STREAM, streamFunction.getInput());
//		assertStatus(END_OF_STREAM, streamFunction.getOutput());
		assertStatus(END_OF_STREAM, consumer);
	}

	@Test
	public void testFunctionConsumerError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamFunction<Integer, Integer> streamFunction = StreamFunction.create(input -> input * input);

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> source1 = StreamProducer.of(1, 2, 3);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		stream(source1, streamFunction.getInput());
		stream(streamFunction.getOutput(),
				consumer.with(decorator((context, dataReceiver) ->
						item -> {
							dataReceiver.onData(item);
							if (list.size() == 2) {
								context.closeWithError(new ExpectedException("Test Exception"));
							}
						})));
		eventloop.run();

		assertEquals(asList(1, 4), list);

		assertStatus(CLOSED_WITH_ERROR, source1);
		assertStatus(CLOSED_WITH_ERROR, consumer);
		assertStatus(CLOSED_WITH_ERROR, streamFunction.getInput());
		assertStatus(CLOSED_WITH_ERROR, streamFunction.getOutput());
	}

	@Test
	public void testFunctionProducerError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamFunction<Integer, Integer> streamFunction = StreamFunction.create(input -> input * input);

		StreamProducer<Integer> source1 = concat(
				StreamProducer.of(1, 2, 3),
				StreamProducer.of(4, 5, 6),
				StreamProducer.closingWithError(new ExpectedException("Test Exception")));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		stream(source1, streamFunction.getInput());
		stream(streamFunction.getOutput(), consumer);
		eventloop.run();

		assertEquals(asList(1, 4, 9, 16, 25, 36), consumer.getList());

		assertStatus(CLOSED_WITH_ERROR, consumer.getProducer());
		assertStatus(CLOSED_WITH_ERROR, streamFunction.getInput());
		assertStatus(CLOSED_WITH_ERROR, streamFunction.getOutput());
	}

	@Test
	public void testIdentity() {
		Function<String, String> function1 = Function.<String>identity();
		Function<Integer, Integer> function2 = Function.<Integer>identity();
		assertTrue((Function) function1 == (Function) function2);
	}

}
