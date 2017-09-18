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
		assertFalse(stage1.isSet());
		assertFalse(stage2.isSet());
		assertFalse(sequenceStage.isSet());

		stage1.set(null);
		eventloop.run();
		assertTrue(stage1.isSet());
		assertFalse(stage2.isSet());
		assertFalse(sequenceStage.isSet());

		stage2.set(null);
		eventloop.run();
		assertTrue(stage1.isSet());
		assertTrue(stage2.isSet());
		assertTrue(sequenceStage.isSet());
	}

}