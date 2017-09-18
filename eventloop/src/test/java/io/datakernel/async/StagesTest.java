package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StagesTest {

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	}

	@Test
	public void testSequence() {
		final SettableStage<Void> stage1 = SettableStage.create();
		final SettableStage<Void> stage2 = SettableStage.create();
		final SettableStage<Void> sequenceStage = SettableStage.create();

		Stages.sequence(asList(() -> stage1, () -> stage2))
				.whenComplete(AsyncCallbacks.forwardTo(sequenceStage));

		eventloop.run();
		assertFalse(stage1.isDone());
		assertFalse(stage2.isDone());
		assertFalse(sequenceStage.isDone());

		stage1.setResult(null);
		eventloop.run();
		assertTrue(stage1.isDone());
		assertFalse(stage2.isDone());
		assertFalse(sequenceStage.isDone());

		stage2.setResult(null);
		eventloop.run();
		assertTrue(stage1.isDone());
		assertTrue(stage2.isDone());
		assertTrue(sequenceStage.isDone());
	}

}