package io.datakernel.datastream.processor;

import io.datakernel.common.exception.ExpectedException;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.datakernel.datastream.StreamSupplier.closingWithError;
import static io.datakernel.datastream.TestStreamTransformers.decorate;
import static io.datakernel.datastream.TestStreamTransformers.randomlySuspending;
import static io.datakernel.datastream.TestUtils.assertClosedWithError;
import static io.datakernel.datastream.TestUtils.assertEndOfStream;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamBufferTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testZeroMinSize() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		StreamBuffer<Integer> buffer = StreamBuffer.create(0, 1);
		await(supplier.transformWith(buffer).streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), consumer.getList());
		assertEndOfStream(supplier, consumer);
		assertEndOfStream(buffer);
	}

	@Test
	public void testBufferedSupplier() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		StreamBuffer<Integer> buffer = StreamBuffer.create(1, 2);
		await(supplier.transformWith(buffer).streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), consumer.getList());
		assertEndOfStream(supplier, consumer);
		assertEndOfStream(buffer);
	}

	@Test
	public void testBufferedConsumer() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		StreamBuffer<Integer> buffer = StreamBuffer.create(1, 2);
		await(supplier.streamTo(consumer.transformWith(buffer).transformWith(randomlySuspending())));

		assertEquals(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), consumer.getList());
		assertEndOfStream(supplier, consumer);
		assertEndOfStream(buffer);
	}

	@Test
	public void testSupplierError() {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> supplier = StreamSupplier.concat(
				StreamSupplier.of(1, 2, 3, 4, 5),
				closingWithError(expectedException),
				StreamSupplier.of(6,7,8,9,10)
		);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		StreamBuffer<Integer> buffer = StreamBuffer.create(1, 2);
		Throwable exception = awaitException(supplier.streamTo(consumer.transformWith(buffer).transformWith(randomlySuspending())));
		assertSame(expectedException, exception);

		assertEquals(asList(1, 2, 3, 4, 5), consumer.getList());
		assertClosedWithError(expectedException, supplier, consumer);
		assertClosedWithError(expectedException, buffer);
	}

	@Test
	public void testConsumerError() {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		StreamBuffer<Integer> buffer = StreamBuffer.create(1, 2);
		Throwable exception = awaitException(supplier.streamTo(consumer
				.transformWith(buffer)
				.transformWith(decorate(promise -> promise.then(
						item -> item == 5 ? Promise.ofException(expectedException) : Promise.of(item))))
				.transformWith(randomlySuspending())));
		assertSame(expectedException, exception);

		assertEquals(asList(1, 2, 3, 4, 5), consumer.getList());
		assertClosedWithError(expectedException, supplier, consumer);
		assertClosedWithError(expectedException, buffer);
	}
}
