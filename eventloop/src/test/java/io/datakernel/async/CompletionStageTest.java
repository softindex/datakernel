package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class CompletionStageTest {

	static CompletionStage<Integer> parseX(String param) {
		SettableStage<Integer> asyncResult = SettableStage.create();
		try {
			Integer result = Integer.valueOf(param);
			asyncResult.setResult(result);
		} catch (NumberFormatException e) {
			asyncResult.setError(e);
		}
		return asyncResult;
	}

	static CompletionStage<String> multiplyX(String string) {
		return parseX(string)
				.thenApply(parsedInt -> parsedInt + "*2 = " + (parsedInt * 2));
	}

	static CompletionStage<Void> multiplyAndPrintX(String string) {
		return multiplyX(string)
				.thenAccept(System.out::println);
	}

	@Test
	public void testStage() throws Exception {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		eventloop.post(() ->
				multiplyAndPrintX("123")
						.whenComplete(($, throwable) -> System.out.println(throwable == null ? "Done" : throwable.toString())));
		eventloop.run();
	}

	@Test
	public void testSimpleResult() throws ExecutionException, InterruptedException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		final CompletableFuture<Integer> future = SettableStage.immediateStage(41)
				.thenApply(integer -> integer + 1)
				.toCompletableFuture();
		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test(expected = RuntimeException.class)
	public void testError() throws Throwable {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		final CompletableFuture<Integer> future = SettableStage.<Integer>immediateFailedStage(new RuntimeException("Test"))
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