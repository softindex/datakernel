package io.datakernel.csp.queue;

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.datakernel.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ChannelBufferTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testBufferStreaming() {
		ChannelBuffer<Integer> buffer = new ChannelBuffer<>(3);
		ChannelSupplier.of(1, 2, 3, 4, 5).streamTo(buffer.getConsumer());
		List<Integer> list = await(buffer.getSupplier().toList());

		assertEquals(asList(1,2,3,4,5), list);
	}
}
