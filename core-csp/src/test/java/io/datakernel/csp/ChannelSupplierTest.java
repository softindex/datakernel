package io.datakernel.csp;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

public class ChannelSupplierTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testToCollector() {
		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.ofIterable(asList(
				ByteBuf.wrapForReading("Test1".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test2".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test3".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test4".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test5".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test6".getBytes(UTF_8))
		));

		ByteBuf resultBuf = await(supplier.toCollector(ByteBufQueue.collector()));
		assertEquals("Test1Test2Test3Test4Test5Test6", resultBuf.asString(UTF_8));
	}

	@Test
	public void testToCollectorWithException() {
		ByteBuf value = ByteBufPool.allocate(100);
		value.put("Test".getBytes(UTF_8));
		Exception exception = new Exception("Test Exception");
		ChannelSupplier<ByteBuf> supplier = ChannelSuppliers.concat(
				ChannelSupplier.of(value),
				ChannelSupplier.ofException(exception)
		);

		Throwable e = awaitException(supplier.toCollector(ByteBufQueue.collector()));
		assertSame(exception, e);
	}

	@Test
	public void testToCollectorMaxSize() {
		ByteBuf byteBuf1 = ByteBuf.wrapForReading("T".getBytes(UTF_8));
		ByteBuf byteBuf2 = ByteBuf.wrapForReading("Te".getBytes(UTF_8));
		ByteBuf byteBuf3 = ByteBuf.wrapForReading("Tes".getBytes(UTF_8));
		ByteBuf byteBuf4 = ByteBuf.wrapForReading("Test".getBytes(UTF_8));

		await(ChannelSupplier.of(byteBuf1).toCollector(ByteBufQueue.collector(2)));
		await(ChannelSupplier.of(byteBuf2).toCollector(ByteBufQueue.collector(2)));
		Throwable e1 = awaitException(ChannelSupplier.of(byteBuf3).toCollector(ByteBufQueue.collector(2)));
		assertThat(e1.getMessage(), containsString("ByteBufQueue exceeds maximum size of 2 bytes"));
		Throwable e2 = awaitException(ChannelSupplier.of(byteBuf4).toCollector(ByteBufQueue.collector(2)));
		assertThat(e2.getMessage(), containsString("ByteBufQueue exceeds maximum size of 2 bytes"));
	}

}
