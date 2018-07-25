package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static org.junit.Assert.assertEquals;

public class CompletionStageTest {

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	}

	@Test
	public void testSimpleResult() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> future = Stage.of(41)
				.thenApply(integer -> integer + 1)
				.toCompletableFuture();
		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test(expected = RuntimeException.class)
	public void testError() throws Throwable {
		CompletableFuture<Integer> future = Stage.<Integer>ofException(new RuntimeException("Test"))
				.thenApply(integer -> integer + 1)
				.toCompletableFuture();
		eventloop.run();

		try {
			future.get();
		} catch (Exception e) {
			throw e.getCause();
		}
	}

	@Test
	public void testLongStagesChainResult() throws ExecutionException, InterruptedException {
		SettableStage<Integer> startStage = new SettableStage<>();
		Stage<Integer> powerOfTwo = startStage;
		for (int i = 0; i < 100_000; i++) {
			if (i % 100 == 0) {
				powerOfTwo = powerOfTwo.post();
			}
			powerOfTwo = powerOfTwo.thenApply(integer -> (integer * 2) % 1000000007);
		}

		CompletableFuture<Integer> future = powerOfTwo.toCompletableFuture();
		startStage.set(1);
		eventloop.run();
		future.get();
	}

	@Test(expected = RuntimeException.class)
	public void testLongStagesChainError() throws Throwable {
		SettableStage<Integer> startStage = new SettableStage<>();
		Stage<Integer> powerOfTwo = startStage;
		for (int i = 0; i < 100_000; i++) {
			if (i % 100 == 0) {
				powerOfTwo = powerOfTwo.post();
			}
			powerOfTwo = powerOfTwo.thenApply(integer -> (integer * 2) % 1000000007);
		}

		CompletableFuture<Integer> future = powerOfTwo.toCompletableFuture();
		startStage.setException(new RuntimeException("Test"));
		eventloop.run();
		try {
			future.get();
		} catch (Exception e) {
			throw e.getCause();
		}
	}

}
