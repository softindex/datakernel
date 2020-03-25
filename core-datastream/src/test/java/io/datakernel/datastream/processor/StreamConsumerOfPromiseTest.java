package io.datakernel.datastream.processor;

import io.datakernel.async.process.AsyncCloseable;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Assert;
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

public class StreamConsumerOfPromiseTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testOfPromise() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> delayedConsumer = StreamConsumerToList.create();
		StreamConsumer<Integer> consumer = StreamConsumer.ofPromise(Promise.complete().async().map($ -> delayedConsumer));
		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEndOfStream(supplier, consumer);
		assertEndOfStream(delayedConsumer);
		assertEquals(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), delayedConsumer.getList());
	}

	@Test
	public void testClosedImmediately() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> delayedConsumer = StreamConsumerToList.create();
		StreamConsumer<Integer> consumer = StreamConsumer.ofPromise(Promise.complete().async().map($ -> delayedConsumer));
		consumer.close();
		Throwable exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(CLOSE_EXCEPTION, exception);
		assertClosedWithError(CLOSE_EXCEPTION, supplier, consumer);
		assertClosedWithError(CLOSE_EXCEPTION, delayedConsumer);
		assertEquals(0, delayedConsumer.getList().size());
	}

	@Test
	public void testClosedDelayedConsumer() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> delayedConsumer = StreamConsumerToList.create();
		delayedConsumer.close();
		StreamConsumer<Integer> consumer = StreamConsumer.ofPromise(Promise.complete().async().map($ -> delayedConsumer));
		Throwable exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(CLOSE_EXCEPTION, exception);
		assertClosedWithError(CLOSE_EXCEPTION, supplier, consumer);
		assertClosedWithError(CLOSE_EXCEPTION, delayedConsumer);
		assertEquals(0, delayedConsumer.getList().size());
	}
}
