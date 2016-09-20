package io.datakernel.stream.net;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.exception.ParseException;
import io.datakernel.stream.net.MessagingSerializers.ByteBufPoolAppendable;
import org.junit.Test;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static org.junit.Assert.assertEquals;

public class ByteBufPoolAppendableTest {
	private static final String HELLO_WORLD = "Hello, World!";

	@Test
	public void testAppendSimple() {
		ByteBufPoolAppendable appendable = new ByteBufPoolAppendable();
		appendable.append(HELLO_WORLD);
		ByteBuf buf = appendable.get();
		assertEquals(0, buf.head());
		assertEquals(13, buf.tail());
		assertEquals(ByteBufStrings.decodeAscii(buf), HELLO_WORLD);
		buf.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testAppendWithResizing() throws ParseException {
		ByteBufPoolAppendable appendable = new ByteBufPoolAppendable(8);
		appendable.append(HELLO_WORLD);
		ByteBuf buf = appendable.get();
		assertEquals(0, buf.head());
		assertEquals(13, buf.tail());
		assertEquals(ByteBufStrings.decodeAscii(buf), HELLO_WORLD);
		buf.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}