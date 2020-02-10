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

import io.datakernel.async.process.AsyncProcess;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.http.TestUtils.AssertingConsumer;
import io.datakernel.promise.Promises;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.csp.ChannelSupplier.ofIterable;
import static io.datakernel.promise.TestUtils.await;
import static org.junit.Assert.assertTrue;

public final class BufsConsumerIntegrationTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final AssertingConsumer consumer = new AssertingConsumer();
	private final ArrayList<ByteBuf> list = new ArrayList<>();
	private final BufsConsumerChunkedEncoder chunkedEncoder = BufsConsumerChunkedEncoder.create();
	private final BufsConsumerChunkedDecoder chunkedDecoder = BufsConsumerChunkedDecoder.create();
	private final BufsConsumerGzipDeflater gzipDeflater = BufsConsumerGzipDeflater.create();
	private final BufsConsumerGzipInflater gzipInflater = BufsConsumerGzipInflater.create();

	@Before
	public void setUp() {
		consumer.reset();
		chunkedEncoder.getOutput().set(chunkedDecoder.getInput().getConsumer());
		chunkedDecoder.getOutput().set(consumer);
		gzipDeflater.getOutput().set(gzipInflater.getInput().getConsumer());
		gzipInflater.getOutput().set(consumer);
	}

	@Test
	public void testEncodeDecodeSingleBuf() {
		writeSingleBuf();
		chunkedEncoder.getInput().set(ofIterable(list));
		doTest(chunkedEncoder, chunkedDecoder);
	}

	@Test
	public void testEncodeDecodeMultipleBufs() {
		writeMultipleBufs();
		chunkedEncoder.getInput().set(ofIterable(list));
		doTest(chunkedEncoder, chunkedDecoder);
	}

	@Test
	public void testGzipGunzipSingleBuf() {
		writeSingleBuf();
		gzipDeflater.getInput().set(ofIterable(list));
		doTest(gzipInflater, gzipDeflater);
	}

	@Test
	public void testGzipGunzipMultipleBufs() {
		writeMultipleBufs();
		gzipDeflater.getInput().set(ofIterable(list));
		doTest(gzipInflater, gzipDeflater);
	}

	private void writeSingleBuf() {
		byte[] data = new byte[1000];
		ThreadLocalRandom.current().nextBytes(data);
		consumer.setExpectedByteArray(data);
		ByteBuf buf = ByteBufPool.allocate(data.length);
		buf.put(data);
		list.add(buf);
	}

	private void writeMultipleBufs() {
		byte[] data = new byte[100_000];
		ThreadLocalRandom.current().nextBytes(data);
		ByteBuf toBeSplitted = ByteBufPool.allocate(data.length);
		ByteBuf expected = ByteBufPool.allocate(data.length);
		toBeSplitted.put(data);
		expected.put(data);
		consumer.setExpectedBuf(expected);
		while (toBeSplitted.readRemaining() != 0) {
			int part = Math.min(ThreadLocalRandom.current().nextInt(100) + 100, toBeSplitted.readRemaining());
			ByteBuf slice = toBeSplitted.slice(part);
			toBeSplitted.moveHead(part);
			list.add(slice);
		}
		toBeSplitted.recycle();
	}

	private void doTest(AsyncProcess process1, AsyncProcess process2) {
		await(Promises.all(process1.getProcessCompletion(), process2.getProcessCompletion()));
		assertTrue(consumer.executed);

	}
}
