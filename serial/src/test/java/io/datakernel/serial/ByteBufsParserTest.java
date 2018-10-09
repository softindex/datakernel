package io.datakernel.serial;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class ByteBufsParserTest {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();
	public final ByteBufQueue queue = new ByteBufQueue();

	@Test
	public void testOfNullTerminatedBytes() throws ParseException {
		ByteBufsParser<ByteBuf> parser = ByteBufsParser.ofNullTerminatedBytes();
		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 0, 4, 5, 6}));
		ByteBuf beforeNull = parser.tryParse(queue);
		ByteBuf afterNull = queue.takeRemaining();

		assertArrayEquals(new byte[]{1, 2, 3}, beforeNull.asArray());
		assertArrayEquals(new byte[]{4, 5, 6}, afterNull.asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{0, 1, 2, 3}));
		beforeNull = parser.tryParse(queue);
		afterNull = queue.takeRemaining();

		assertArrayEquals(new byte[]{}, beforeNull.asArray());
		assertArrayEquals(new byte[]{1, 2, 3}, afterNull.asArray());
	}
}
