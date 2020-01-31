package io.datakernel.datastream;

import io.datakernel.common.ref.RefInt;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static io.datakernel.datastream.TestStreamSuppliers.errorDecorator;
import static io.datakernel.datastream.TestUtils.assertClosedWithError;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

public class StreamSuppliersTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testErrorDecorator() {
		IllegalArgumentException exception = new IllegalArgumentException("TestException");
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed())
				.transformWith(errorDecorator(k -> k.equals(5) ? exception : null));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		Throwable e = awaitException(supplier.streamTo(consumer));

		assertSame(exception, e);
		assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() {
		IllegalArgumentException exception = new IllegalArgumentException("TestException");
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed())
				.transformWith(errorDecorator(k -> k.equals(5) ? exception : null));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		Throwable e = awaitException(supplier.streamTo(consumer));

		assertSame(exception, e);
		assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

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
