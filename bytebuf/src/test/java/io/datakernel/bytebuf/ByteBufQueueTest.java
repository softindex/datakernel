package io.datakernel.bytebuf;

import org.junit.Before;
import org.junit.Test;

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
		queue.asIterator();
		assertEquals(0, queue.remainingBufs());
	}
}
