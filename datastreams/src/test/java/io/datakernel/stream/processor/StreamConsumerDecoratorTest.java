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
import io.datakernel.stream.StreamConsumerDecorator;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.DataStreams.stream;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.TestUtils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamConsumerDecoratorTest {
	@Test
	public void test2() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		List<Integer> list = new ArrayList<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.oneByOne(list);
		StreamConsumerDecorator<Integer> consumerDecorator = new StreamConsumerDecorator<Integer>() {};
		consumerDecorator.setActualConsumer(consumer);

		StreamProducer<Integer> producer = StreamProducer.concat(
				StreamProducer.of(1, 2, 3),
				StreamProducer.closingWithError(new Exception("Test Exception")));

		stream(producer, consumerDecorator);
		eventloop.run();

		assertEquals(list, asList(1, 2, 3));
		assertStatus(CLOSED_WITH_ERROR, producer);
		assertStatus(CLOSED_WITH_ERROR, consumer);
	}

	@Test
	public void test1() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamConsumerWithResult<Integer, List<Integer>> consumer = new StreamConsumerToList<>();
		CompletableFuture<List<Integer>> listFuture = consumer.getResult().toCompletableFuture();
		StreamConsumerDecorator<Integer> decorator = new StreamConsumerDecorator<Integer>() {};
		decorator.setActualConsumer(consumer);
		StreamProducer<Integer> producer = StreamProducer.of(1, 2, 3, 4, 5);

		stream(producer, decorator);

		eventloop.run();

		assertEquals(listFuture.get(), asList(1, 2, 3, 4, 5));
		assertStatus(END_OF_STREAM, producer);
		assertStatus(END_OF_STREAM, consumer);
	}

}
