package io.datakernel.async.service;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.RetryPolicy;
import io.datakernel.promise.SettablePromise;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class EventloopTaskSchedulerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testStopping() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettablePromise<Void> settablePromise = new SettablePromise<>();
		settablePromise.whenComplete(eventloop::completeExternalTask);

		EventloopTaskScheduler scheduler = EventloopTaskScheduler.create(eventloop, () -> settablePromise)
				.withSchedule(EventloopTaskScheduler.Schedule.immediate());
		scheduler.start();

		Promise.complete().async().whenComplete(() -> {
			Promise<Void> stopPromise = scheduler.stop();
			assertFalse(stopPromise.isComplete());
			settablePromise.set(null);
			assertTrue(stopPromise.isComplete());
		});

		eventloop.run();
	}

	@Test
	public void testStoppingWithRetryPolicy() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettablePromise<Void> settablePromise = new SettablePromise<>();
		settablePromise.whenComplete(eventloop::completeExternalTask);

		EventloopTaskScheduler scheduler = EventloopTaskScheduler.create(eventloop, () -> Promise.ofException(new Exception()))
				.withRetryPolicy(RetryPolicy.immediateRetry())
				.withSchedule(EventloopTaskScheduler.Schedule.immediate());
		scheduler.start();

		Promise.complete().async().whenComplete(() -> {
			Promise<Void> stopPromise = scheduler.stop()
					.whenComplete(eventloop::completeExternalTask);
			assertFalse(stopPromise.isComplete());
			settablePromise.set(null);
		});

		eventloop.run();
	}
}
