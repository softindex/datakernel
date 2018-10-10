/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.http.stream;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.http.TestUtils.AssertingConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.ActiveStagesRule;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.datakernel.test.TestUtils.assertComplete;

public class BufsConsumerChunkedEncoderTest {
	@Rule
	public final ByteBufRule byteBufRule = new ByteBufRule();
	@Rule
	public ActiveStagesRule activeStagesRule = new ActiveStagesRule();
	private final Random RANDOM = new Random();
	private final List<ByteBuf> list = new ArrayList<>();
	private final Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	private final AssertingConsumer consumer = new AssertingConsumer();
	private final BufsConsumerChunkedEncoder chunkedEncoder = BufsConsumerChunkedEncoder.create();

	@Before
	public void setUp() {
		consumer.reset();
		chunkedEncoder.getOutput().set(consumer);
	}

	@Test
	public void testEncoderSingleBuf() {
		byte[] chunkData = new byte[100];
		RANDOM.nextBytes(chunkData);
		ByteBuf buf = ByteBufPool.allocate(chunkData.length);
		ByteBuf expected = ByteBufPool.allocate(200);
		buf.put(chunkData);
		list.add(buf);
		expected.put((Integer.toHexString(100) + "\r\n").getBytes());
		expected.put(chunkData);
		expected.put("\r\n0\r\n\r\n".getBytes());
		consumer.setExpectedBuf(expected);
		doTest();
	}

	@Test
	public void testEncodeWithEmptyBuf() {
		byte[] chunkData = new byte[100];
		RANDOM.nextBytes(chunkData);
		ByteBuf buf = ByteBufPool.allocate(chunkData.length);
		buf.put(chunkData);
		ByteBuf expected = ByteBufPool.allocate(200);
		ByteBuf empty = ByteBufPool.allocate(100);
		list.add(buf);
		list.add(empty);
		expected.put((Integer.toHexString(100) + "\r\n").getBytes());
		expected.put(chunkData);
		expected.put("\r\n0\r\n\r\n".getBytes());
		consumer.setExpectedBuf(expected);

		doTest();
	}

	@Test
	public void testWithChunkedOutputStream() throws IOException {
		byte[] chunkData = new byte[1000];
		RANDOM.nextBytes(chunkData);
		consumer.setExpectedByteArray(BufsConsumerChunkedDecoderTest.encode(chunkData, true));
		ByteBuf buf = ByteBufPool.allocate(chunkData.length);
		buf.put(chunkData);
		list.add(buf);
		doTest();
	}

	private void doTest() {
		chunkedEncoder.getInput().set(SerialSupplier.ofIterable(list));
		chunkedEncoder.getProcessResult()
				.whenComplete(assertComplete());
		eventloop.run();
	}

}
