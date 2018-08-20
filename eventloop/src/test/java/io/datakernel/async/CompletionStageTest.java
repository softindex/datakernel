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

}
