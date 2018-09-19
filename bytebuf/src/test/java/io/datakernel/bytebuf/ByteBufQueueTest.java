package io.datakernel.bytebuf;

import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class ByteBufQueueTest {
	private final ByteBufQueue queue = new ByteBufQueue();

	@Before
	public void setUp() {
		queue.recycle();
		queue.add(ByteBufStrings.wrapAscii("First"));
		queue.add(ByteBufStrings.wrapAscii("Second"));
		queue.add(ByteBufStrings.wrapAscii("Third"));
		queue.add(ByteBufStrings.wrapAscii("Fourth"));

	}

	@Test
	public void testAsIterator() {
		Iterator<ByteBuf> iterator = queue.asIterator();
		assertEquals(4, queue.remainingBufs());
		ByteBuf next = iterator.next();
		next.set(0, (byte) 0);
		assertEquals("First", queue.take().asString(UTF_8));
	}

	@Test
	public void testToIterator() {
		queue.asIterator();
		assertEquals(0, queue.remainingBufs());
	}
}
