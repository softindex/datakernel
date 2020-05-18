package io.datakernel.datastream.processor;

import io.datakernel.common.exception.ExpectedException;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.TestUtils.CountingStreamConsumer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static io.datakernel.datastream.TestStreamTransformers.decorate;
import static io.datakernel.datastream.TestStreamTransformers.randomlySuspending;
import static io.datakernel.datastream.TestUtils.assertClosedWithError;
import static io.datakernel.datastream.TestUtils.assertEndOfStream;
import static io.datakernel.eventloop.Eventloop.initWithEventloop;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@SuppressWarnings("Convert2MethodRef")
public class StreamConsumerOfAnotherEventloopTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private Eventloop anotherEventloop;

	@Before
	public void setUp() {
		anotherEventloop = Eventloop.create();
		anotherEventloop.keepAlive(true);
		new Thread(anotherEventloop, "another").start();
	}

	@After
	public void tearDown() {
		anotherEventloop.execute(() -> anotherEventloop.keepAlive(false));
	}

	@Test
	public void testStreaming() throws ExecutionException, InterruptedException {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5);
		StreamConsumerToList<Integer> anotherEventloopConsumer = initWithEventloop(anotherEventloop, () -> StreamConsumerToList.create());
		StreamConsumer<Integer> consumer = StreamConsumer.ofAnotherEventloop(anotherEventloop, anotherEventloopConsumer);

		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(asList(1, 2, 3, 4, 5), anotherEventloopConsumer.getList());
		assertEndOfStream(supplier, consumer);
		anotherEventloop.submit(() -> assertEndOfStream(anotherEventloopConsumer)).get();
	}

	@Test
	public void testSupplierException() throws ExecutionException, InterruptedException {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> supplier = StreamSupplier.concat(StreamSupplier.of(1, 2, 3), StreamSupplier.closingWithError(expectedException), StreamSupplier.of(4, 5, 6));
		StreamConsumerToList<Integer> anotherEventloopConsumer = initWithEventloop(anotherEventloop, () -> StreamConsumerToList.create());
		StreamConsumer<Integer> consumer = StreamConsumer.ofAnotherEventloop(anotherEventloop, anotherEventloopConsumer);

		Throwable exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(expectedException, exception);
		assertClosedWithError(expectedException, supplier, consumer);
		anotherEventloop.submit(() -> assertClosedWithError(expectedException, anotherEventloopConsumer)).get();
	}

	@Test
	public void testConsumerException() throws ExecutionException, InterruptedException {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5);
		StreamConsumerToList<Integer> anotherEventloopConsumer = initWithEventloop(anotherEventloop, () -> StreamConsumerToList.create());
		StreamConsumer<Integer> consumer = StreamConsumer.ofAnotherEventloop(anotherEventloop, anotherEventloopConsumer)
				.transformWith(decorate(promise -> promise
						.then(item -> item == 4 ? Promise.ofException(expectedException) : Promise.of(item))))
				.transformWith(randomlySuspending());

		Throwable exception = awaitException(supplier.streamTo(consumer));

		assertSame(expectedException, exception);
		assertClosedWithError(expectedException, supplier, consumer);
		anotherEventloop.submit(() -> assertClosedWithError(expectedException, anotherEventloopConsumer)).get();
	}

	@Test
	public void testForOutOfMemoryError() throws ExecutionException, InterruptedException {
		int nItems = 10000;
		StreamSupplier<byte[]> supplier = StreamSupplier.ofStream(Stream.generate(() -> new byte[1024 * 1024]).limit(nItems));
		CountingStreamConsumer<byte[]> anotherEventloopConsumer = initWithEventloop(anotherEventloop, () -> new CountingStreamConsumer<>());
		StreamConsumer<byte[]> consumer = StreamConsumer.ofAnotherEventloop(anotherEventloop, anotherEventloopConsumer);

		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(nItems, anotherEventloopConsumer.getCount());
		assertEndOfStream(supplier, consumer);
		anotherEventloop.submit(() -> assertEndOfStream(anotherEventloopConsumer)).get();
	}
}
