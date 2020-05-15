package io.datakernel.csp.process;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.common.MemSize;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.stream.Stream;

import static io.datakernel.promise.TestUtils.await;

public class ChannelByteChunkerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testForStackOverflow() {
		ChannelByteChunker channelByteChunker = ChannelByteChunker.create(MemSize.of(10_000), MemSize.of(10_000));
		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.ofStream(Stream.generate(() -> ByteBufStrings.wrapAscii("a")).limit(10_000))
				.transformWith(channelByteChunker);
		await(supplier.streamTo(ChannelConsumer.ofConsumer(ByteBuf::recycle)));
	}
}
