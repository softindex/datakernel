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
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.http.TestUtils.AssertingConsumer;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.promise.TestUtils.await;

public final class BufsConsumerChunkedEncoderTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final List<ByteBuf> list = new ArrayList<>();
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
		ThreadLocalRandom.current().nextBytes(chunkData);
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
		ThreadLocalRandom.current().nextBytes(chunkData);
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

	private void doTest() {
		chunkedEncoder.getInput().set(ChannelSupplier.ofIterable(list));
		await(chunkedEncoder.getProcessCompletion());
	}
}
