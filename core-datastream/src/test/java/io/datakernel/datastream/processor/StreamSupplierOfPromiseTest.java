package io.datakernel.datastream.processor;

import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.datakernel.async.process.AsyncCloseable.CLOSE_EXCEPTION;
import static io.datakernel.datastream.TestStreamTransformers.randomlySuspending;
import static io.datakernel.datastream.TestUtils.assertClosedWithError;
import static io.datakernel.datastream.TestUtils.assertEndOfStream;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamSupplierOfPromiseTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testOfPromise() {
		StreamSupplier<Integer> delayedSupplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamSupplier<Integer> supplier = StreamSupplier.ofPromise(Promise.complete().async().map($ -> delayedSupplier));
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEndOfStream(supplier, consumer);
		assertEndOfStream(delayedSupplier);
		assertEquals(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), consumer.getList());
	}

	@Test
	public void testClosedImmediately() {
		StreamSupplier<Integer> delayedSupplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamSupplier<Integer> supplier = StreamSupplier.ofPromise(Promise.complete().async().map($ -> delayedSupplier));
		supplier.close();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Throwable exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(CLOSE_EXCEPTION, exception);
		assertClosedWithError(CLOSE_EXCEPTION, supplier, consumer);
		assertClosedWithError(CLOSE_EXCEPTION, delayedSupplier);
		assertEquals(0, consumer.getList().size());
	}

	@Test
	public void testClosedDelayedSupplier() {
		StreamSupplier<Integer> delayedSupplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		delayedSupplier.close();
		StreamSupplier<Integer> supplier = StreamSupplier.ofPromise(Promise.complete().async().map($ -> delayedSupplier));
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Throwable exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(CLOSE_EXCEPTION, exception);
		assertClosedWithError(CLOSE_EXCEPTION, supplier, consumer);
		assertClosedWithError(CLOSE_EXCEPTION, delayedSupplier);
		assertEquals(0, consumer.getList().size());
	}

}
