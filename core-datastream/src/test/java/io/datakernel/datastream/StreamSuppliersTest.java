package io.datakernel.datastream;

import io.datakernel.common.ref.RefInt;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class StreamSuppliersTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testSupplierSupplier() {
		List<Integer> actual = new ArrayList<>();
		RefInt count = new RefInt(-1);
		await(StreamSupplier.ofSupplier(
				() -> {
					if (count.get() == 10) {
						return null;
					}
					return count.inc();
				})
				.streamTo(StreamConsumerToList.create(actual)));

		assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), actual);
	}
}
