package io.datakernel.datastream;

import io.datakernel.common.exception.ExpectedException;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.datastream.AbstractStreamSupplierTest.StatusAssertingSupplier.Status.*;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public final class AbstractStreamSupplierTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testSendingBeforeBound() {
		AbstractStreamSupplier<Integer> supplier = new AbstractStreamSupplier<Integer>() {};
		supplier.send(1);
		supplier.send(2);
		supplier.send(3);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.send(4);
		supplier.send(5);
		Promise<Void> eosPromise = supplier.sendEndOfStream();
		await(streamPromise, eosPromise);
		assertEquals(asList(1, 2, 3, 4, 5), consumer.getList());
	}

	@Test
	public void testSendingEndOfStreamBeforeBound() {
		AbstractStreamSupplier<Integer> supplier = new AbstractStreamSupplier<Integer>() {};
		supplier.send(1);
		supplier.send(2);
		supplier.send(3);
		Promise<Void> eosPromise = supplier.sendEndOfStream();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		await(streamPromise, eosPromise);
		assertEquals(asList(1, 2, 3), consumer.getList());
	}

	@Test
	public void testClosingBeforeBound() {
		ExpectedException expectedException = new ExpectedException("test");
		AbstractStreamSupplier<Integer> supplier = new AbstractStreamSupplier<Integer>() {};
		supplier.send(1);
		supplier.send(2);
		supplier.closeEx(expectedException);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		Throwable exception = awaitException(streamPromise);
		assertSame(expectedException, exception);
		supplier.onAcknowledge();
	}

	@Test
	public void testStatusesOnCompletedStream() {
		StatusAssertingSupplier<Integer> supplier = new StatusAssertingSupplier<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.send(1);
		supplier.send(2);
		supplier.send(3);
		await(supplier.sendEndOfStream(), streamPromise);

		assertEquals(asList(1, 2, 3), consumer.getList());
		supplier.assertStatuses();
	}

	@Test
	public void testStatusesOnCompletedStreamBoundLater() {
		StatusAssertingSupplier<Integer> supplier = new StatusAssertingSupplier<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		supplier.send(1);
		supplier.send(2);
		supplier.send(3);
		await(supplier.sendEndOfStream(), supplier.streamTo(consumer));

		assertEquals(asList(1, 2, 3), consumer.getList());
		supplier.assertStatuses();
	}

	@Test
	public void testStatusesOnCompletedLaterStreamBoundLater() {
		StatusAssertingSupplier<Integer> supplier = new StatusAssertingSupplier<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		supplier.send(1);
		supplier.send(2);
		supplier.send(3);
		Eventloop.getCurrentEventloop().post(supplier::sendEndOfStream);
		await(supplier.streamTo(consumer));

		assertEquals(asList(1, 2, 3), consumer.getList());
		supplier.assertStatuses();
	}

	@Test
	public void testStatusesOnCompletedLaterStream() {
		StatusAssertingSupplier<Integer> supplier = new StatusAssertingSupplier<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.send(1);
		supplier.send(2);
		supplier.send(3);

		Eventloop.getCurrentEventloop().post(supplier::sendEndOfStream);

		await(streamPromise);

		assertEquals(asList(1, 2, 3), consumer.getList());
		supplier.assertStatuses();
	}

	@Test
	public void testStatusesOnCompletedExceptionallyStream() {
		ExpectedException expectedException = new ExpectedException();
		StatusAssertingSupplier<Integer> supplier = new StatusAssertingSupplier<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.send(1);
		supplier.closeEx(expectedException);
		Throwable exception = awaitException(streamPromise);
		assertSame(expectedException, exception);

		assertEquals(singletonList(1), consumer.getList());
		supplier.assertStatuses();
	}

	@Test
	public void testStatusesOnCompletedLaterExceptionallyStream() {
		ExpectedException expectedException = new ExpectedException();
		StatusAssertingSupplier<Integer> supplier = new StatusAssertingSupplier<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.send(1);
		Eventloop.getCurrentEventloop().post(() -> supplier.closeEx(expectedException));
		Throwable exception = awaitException(streamPromise);
		assertSame(expectedException, exception);

		assertEquals(singletonList(1), consumer.getList());
		supplier.assertStatuses();
	}

	static final class StatusAssertingSupplier<T> extends AbstractStreamSupplier<T> {
		enum Status {
			START, RESUME, SUSPEND, ACKNOWLEDGE, ERROR, COMPLETE, CLEANUP
		}

		List<Status> statuses = new ArrayList<>();

		void assertStatuses() {
			System.out.println(statuses);
			int i = 0;
			assertEquals(START, statuses.get(i++));
			for (; i < statuses.size() - 3; i++) {
				Status status = statuses.get(i);
				assertTrue(RESUME.equals(status) || SUSPEND.equals(status));
			}
			Status status = statuses.get(i++);
			assertTrue(ACKNOWLEDGE.equals(status) || ERROR.equals(status));
			assertEquals(COMPLETE, statuses.get(i++));
			assertEquals(CLEANUP, statuses.get(i));
		}

		@Override
		protected void onStarted() {
			statuses.add(START);
		}

		@Override
		protected void onResumed() {
			statuses.add(RESUME);
		}

		@Override
		protected void onSuspended() {
			statuses.add(SUSPEND);
		}

		@Override
		protected void onAcknowledge() {
			statuses.add(ACKNOWLEDGE);
		}

		@Override
		protected void onError(Throwable e) {
			statuses.add(ERROR);
		}

		@Override
		protected void onComplete() {
			statuses.add(COMPLETE);
		}

		@Override
		protected void onCleanup() {
			statuses.add(CLEANUP);
		}
	}
}
