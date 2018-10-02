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

package io.datakernel.bytebuf;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static org.junit.Assert.*;

public class ByteBufTest {
	private static final byte[] BYTES = new byte[]{'T', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'};

	@Before
	public void clearByteBufPool() {
		ByteBufPool.clear();
	}

	@After
	public void checkByteBufPool() {
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testSlice() {
		byte[] bytes = ByteBufStrings.encodeAscii("Hello, World");
		ByteBuf buf = ByteBuf.wrapForReading(bytes);

		ByteBuf slice = buf.slice(7, 5);

		assertFalse(buf == slice);
		assertEquals("World", slice.toString());

		buf = createEmptyByteBufOfSize(16);
		buf.put(bytes);

		slice = buf.slice();

		assertFalse(buf == slice);
		assertEquals("Hello, World", slice.toString());
	}

	@Test
	public void testEditing() {
		ByteBuf buf = createEmptyByteBufOfSize(256);
		assertEquals(0, buf.readPosition());

		buf.put((byte) 'H');
		assertEquals(1, buf.writePosition());
		assertEquals('H', buf.at(0));

		buf.put(new byte[]{'e', 'l', 'l', 'o'});
		buf.put(new byte[]{';', ' ', ',', ' ', '.', ' ', '!', ' '}, 2, 2);
		assertEquals(7, buf.writePosition());

		ByteBuf worldBuf = ByteBuf.wrapForReading(new byte[]{'W', 'o', 'r', 'l', 'd', '!'});
		buf.put(worldBuf);

		assertEquals(worldBuf.limit(), worldBuf.readPosition());
		assertFalse(worldBuf.canWrite());
		assertEquals(13, buf.writePosition());

		ByteBuf slice = buf.slice();
		ByteBuf newBuf = createEmptyByteBufOfSize(slice.limit());
		slice.drainTo(newBuf, 10);
		assertEquals(10, slice.readPosition());
		assertEquals(10, newBuf.writePosition());

		slice.drainTo(newBuf, 3);

		assertEquals("Hello, World!", newBuf.toString());
	}

	@Test
	public void transformsToByteBufferInReadMode() {
		ByteBuf buf = createEmptyByteBufOfSize(8);
		buf.writePosition(5);
		buf.readPosition(2);

		ByteBuffer buffer = buf.toReadByteBuffer();

		assertEquals(2, buffer.position());
		assertEquals(5, buffer.limit());
	}

	@Test
	public void transformsToByteBufferInWriteMode() {
		ByteBuf buf = createEmptyByteBufOfSize(8);
		buf.writePosition(5);
		buf.readPosition(2);

		ByteBuffer buffer = buf.toWriteByteBuffer();

		assertEquals(5, buffer.position());
		assertEquals(8, buffer.limit());
	}

	@Test
	public void testPoolAndRecycleMechanism() {
		int size = 500;
		ByteBuf buf = ByteBufPool.allocate(size);
		assertNotEquals(size, buf.limit()); // {expected to create 2^N sized bufs only, 500 not in {a}|a == 2^N } => size != limit
		assertEquals(512, buf.limit());

		buf.put(BYTES);

		buf.recycle();

		try {
			buf.put((byte) 'a');
		} catch (AssertionError e) {
			assertEquals(AssertionError.class, e.getClass());
		}

		buf = ByteBufPool.allocate(300);
		buf.writePosition(BYTES.length);
		byte[] bytes = new byte[BYTES.length];
		buf.drainTo(bytes, 0, bytes.length);
		assertArrayEquals(bytes, BYTES);

		buf.recycle();
	}

	@Test
	public void testSliceAndRecycleMechanism() {
		ByteBuf buf = ByteBufPool.allocate(5);
		ByteBuf slice0 = buf.slice(1, 3);
		ByteBuf slice01 = slice0.slice(2, 1);
		ByteBuf slice1 = buf.slice(4, 1);

		assertTrue(buf.canWrite());
		slice1.recycle();
		assertTrue(buf.canWrite());
		slice0.recycle();
		assertTrue(buf.canWrite());
		slice01.recycle();
		buf.recycle();
	}

	@Test
	public void testViews() {
		// emulate engine that receives randomly sized bufs from `net` and sends them to some `consumer`

		class MockConsumer {
			private int i = 0;

			private void consume(ByteBuf buf) {
				assertEquals("Test message " + i++, buf.toString());
				buf.recycle();
			}
		}

		MockConsumer consumer = new MockConsumer();
		for (int i = 0; i < 100; i++) {
			ByteBuf buf = ByteBufPool.allocate(32);
			ByteBuffer buffer = buf.toWriteByteBuffer();
			buffer.put(("Test message " + i).getBytes());
			buffer.flip();
			buf.writePosition(buffer.limit());
			buf.readPosition(buffer.position());
			consumer.consume(buf.slice());
			buf.recycle();
		}
	}

	@Test
	public void testConcat() {
		ByteBuf buf = ByteBufPool.allocate(64);
		buf.put(BYTES);

		ByteBuf secondBuf = ByteBufPool.allocate(32);
		secondBuf.put(BYTES);

		buf = ByteBufPool.append(buf, secondBuf.slice());
		buf = ByteBufPool.append(buf, secondBuf.slice());
		buf = ByteBufPool.append(buf, secondBuf.slice());
		buf = ByteBufPool.append(buf, secondBuf.slice());
		buf = ByteBufPool.append(buf, secondBuf.slice());

		assertEquals(new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES), buf.toString());

		buf.recycle();
		secondBuf.recycle();
	}

	@Test
	public void testSkipAndAdvance() {
		ByteBuf buf = createEmptyByteBufOfSize(5);
		buf.put(new byte[]{'a', 'b', 'c'});
		buf.moveReadPosition(2);
		assertEquals('c', buf.get());
		buf.array[3] = 'd';
		buf.array[4] = 'e';
		assertFalse(buf.canRead());
		buf.moveWritePosition(2);
		assertTrue(buf.canRead());
		byte[] bytes = new byte[2];
		buf.drainTo(bytes, 0, 2);
		assertTrue(Arrays.equals(new byte[]{'d', 'e'}, bytes));
	}

	@Test
	public void testGet() {
		ByteBuf buf = createEmptyByteBufOfSize(3);
		buf.put(new byte[]{'a', 'b', 'c'});

		assertEquals('a', buf.get());
		assertEquals('b', buf.get());
		assertEquals('c', buf.get());
	}

	@Test
	public void testFind() {
		ByteBuf buf = createEmptyByteBufOfSize(BYTES.length);
		buf.put(BYTES);

		assertEquals(5, buf.find((byte) 'm'));
		assertEquals(-1, buf.find(new byte[]{'T', 'e', 's', 's'}));
		assertEquals(1, buf.find(new byte[]{'T', 'e', 's', 's'}, 1, 2));
	}

	@Test
	public void testClearPool() {
		ByteBufPool.clear();
		List<String> cleared = ByteBufPool.getStats().getPoolSlabs();
		assertEquals("32768,0,0,0", cleared.get(16));
		int size = 10;
		ByteBuf[] bufs = new ByteBuf[size];
		for (int i = 0; i < size; i++) {
			bufs[i] = ByteBufPool.allocate(30_000);
		}
		assertNotEquals(cleared.get(16), ByteBufPool.getStats().getPoolSlabs().get(16));
		for (int i = 0; i < size; i++) {
			bufs[i].recycle();
		}
		ByteBufPool.getStats().clearPool();
		assertEquals(cleared, ByteBufPool.getStats().getPoolSlabs());
	}

	private ByteBuf createEmptyByteBufOfSize(int size) {
		return ByteBuf.wrapForWriting(new byte[size]);
	}
}
