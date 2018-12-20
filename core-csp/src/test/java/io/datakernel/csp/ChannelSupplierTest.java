package io.datakernel.csp;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public class ChannelSupplierTest {
	@Test
	public void testToCollector() {
		List<ByteBuf> list = asList(
				ByteBuf.wrapForReading("Test1".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test2".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test3".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test4".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test5".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test6".getBytes(UTF_8))
		);

		ChannelSupplier.ofIterable(list).toCollector(ByteBufQueue.collector(Integer.MAX_VALUE))
				.whenComplete(assertComplete(resultBuf -> assertEquals("Test1Test2Test3Test4Test5Test6", resultBuf.asString(UTF_8))));
	}

	@Test
	public void testToCollectorWithException() {
		ByteBuf value = ByteBufPool.allocate(100);
		value.put("Test".getBytes(UTF_8));
		ChannelSupplier<ByteBuf> supplier = ChannelSuppliers.concat(
				ChannelSupplier.of(value),
				ChannelSupplier.ofException(new Exception("Test Exception"))
		);

		supplier.toCollector(ByteBufQueue.collector(Integer.MAX_VALUE))
				.whenComplete(assertFailure("Test Exception"));
	}

	@Test
	public void testToCollectorMaxSize() {
		ByteBuf byteBuf1 = ByteBuf.wrapForReading("T".getBytes(UTF_8));
		ByteBuf byteBuf2 = ByteBuf.wrapForReading("Te".getBytes(UTF_8));
		ByteBuf byteBuf3 = ByteBuf.wrapForReading("Tes".getBytes(UTF_8));
		ByteBuf byteBuf4 = ByteBuf.wrapForReading("Test".getBytes(UTF_8));

		ChannelSupplier.of(byteBuf1).toCollector(ByteBufQueue.collector(2))
				.whenComplete(assertComplete());
		ChannelSupplier.of(byteBuf2).toCollector(ByteBufQueue.collector(2))
				.whenComplete(assertComplete());
		ChannelSupplier.of(byteBuf3).toCollector(ByteBufQueue.collector(2))
				.whenComplete(assertFailure("ByteBufQueue exceeds maximum size of 2 bytes"));
		ChannelSupplier.of(byteBuf4).toCollector(ByteBufQueue.collector(2))
				.whenComplete(assertFailure("ByteBufQueue exceeds maximum size of 2 bytes"));
	}


}
