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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamProducer;
import io.datakernel.util.MemSize;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serializer.asm.BufferSerializers.INT_SERIALIZER;
import static io.datakernel.stream.StreamConsumers.oneByOne;
import static io.datakernel.stream.StreamConsumers.randomlySuspending;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.TestUtils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamSerializerTest {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test1() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamProducer<Integer> producer = StreamProducer.of(10, 20, 30, 40);
		StreamBinarySerializer<Integer> serializerStream = StreamBinarySerializer.create(INT_SERIALIZER)
				.withInitialBufferSize(MemSize.of(14))
				.withMaxMessageSize(MemSize.of(14));
		StreamConsumerToList<ByteBuf> consumer = StreamConsumerToList.create();

		producer.with(serializerStream).streamTo(
				consumer.with(randomlySuspending()));

		eventloop.run();
		List<ByteBuf> result = consumer.getList();
		Assert.assertEquals(2, result.size());
		Assert.assertEquals(16, result.get(0).limit());
		Assert.assertEquals(16, result.get(1).limit());
		Assert.assertEquals(10, result.get(0).array()[4]);
		Assert.assertEquals(20, result.get(0).array()[9]);
		Assert.assertEquals(30, result.get(0).array()[14]);
		Assert.assertEquals(40, result.get(1).array()[4]);
		for (ByteBuf buf : result) {
			buf.recycle();
		}

		assertStatus(END_OF_STREAM, serializerStream.getInput());
		assertStatus(END_OF_STREAM, serializerStream.getOutput());
	}

	@Test
	public void test2() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> producer = StreamProducer.of(1, 2, 3);
		StreamBinarySerializer<Integer> serializerStream = StreamBinarySerializer.create(INT_SERIALIZER)
				.withInitialBufferSize(MemSize.of(1));
		StreamBinaryDeserializer<Integer> deserializerStream = StreamBinaryDeserializer.create(INT_SERIALIZER);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		producer.with(serializerStream).with(deserializerStream).streamTo(
				consumer.with(oneByOne()));

		eventloop.run();
		assertEquals(asList(1, 2, 3), consumer.getList());
		assertStatus(END_OF_STREAM, producer);

		assertStatus(END_OF_STREAM, serializerStream.getInput());
		assertStatus(END_OF_STREAM, serializerStream.getOutput());

		assertStatus(END_OF_STREAM, deserializerStream.getInput());
		assertStatus(END_OF_STREAM, deserializerStream.getOutput());
	}

	@Test
	public void testProducerWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> producer = StreamProducer.closingWithError(new ExpectedException("Test Exception"));
		StreamBinarySerializer<Integer> serializerStream = StreamBinarySerializer.create(INT_SERIALIZER)
				.withInitialBufferSize(MemSize.of(1));
		StreamBinaryDeserializer<Integer> deserializerStream = StreamBinaryDeserializer.create(INT_SERIALIZER);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		producer.with(serializerStream).with(deserializerStream).streamTo(
				consumer.with(oneByOne()));

		eventloop.run();
		assertStatus(CLOSED_WITH_ERROR, consumer);
//		assertStatus(CLOSED_WITH_ERROR, producer);

		assertStatus(CLOSED_WITH_ERROR, serializerStream.getInput());
		assertStatus(CLOSED_WITH_ERROR, serializerStream.getOutput());

		assertStatus(CLOSED_WITH_ERROR, deserializerStream.getInput());
		assertStatus(CLOSED_WITH_ERROR, deserializerStream.getOutput());
	}

}
