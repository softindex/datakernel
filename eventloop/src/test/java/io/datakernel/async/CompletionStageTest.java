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

	static Stage<Integer> parseX(String param) {
		SettableStage<Integer> asyncResult = SettableStage.create();
		try {
			Integer result = Integer.valueOf(param);
			asyncResult.set(result);
		} catch (NumberFormatException e) {
			asyncResult.setException(e);
		}
		return asyncResult;
	}

	static Stage<String> multiplyX(String string) {
		return parseX(string)
				.thenApply(parsedInt -> parsedInt + "*2 = " + (parsedInt * 2));
	}

	static Stage<Void> multiplyAndPrintX(String string) {
		return multiplyX(string)
				.thenAccept(System.out::println);
	}

	@Test
	public void testStage() throws Exception {
		eventloop.post(() ->
				multiplyAndPrintX("123")
						.whenComplete(($, throwable) -> System.out.println(throwable == null ? "Done" : throwable.toString())));
		eventloop.run();
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
		SettableStage<Integer> startStage = SettableStage.create();
		Stage<Integer> powerOfTwo = startStage;
		for (int i = 0; i < 100_000; i++) {
			powerOfTwo = powerOfTwo.then(i % 100 == 0 ? NextStage.async() : null).thenApply(integer -> (integer * 2) % 1000000007);
		}

		CompletableFuture<Integer> future = powerOfTwo.toCompletableFuture();
		startStage.set(1);
		eventloop.run();
		future.get();
	}

	@Test(expected = RuntimeException.class)
	public void testLongStagesChainError() throws Throwable {
		SettableStage<Integer> startStage = SettableStage.create();
		Stage<Integer> powerOfTwo = startStage;
		for (int i = 0; i < 100_000; i++) {
			powerOfTwo = powerOfTwo.then(i % 100 == 0 ? NextStage.async() : null).thenApply(integer -> (integer * 2) % 1000000007);
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