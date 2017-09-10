package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class CompletionStageTest {

	private Eventloop eventloop;
	private ExecutorService executor;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		executor = Executors.newCachedThreadPool();
	}

	static CompletionStage<Integer> parseX(String param) {
		SettableStage<Integer> asyncResult = SettableStage.create();
		try {
			Integer result = Integer.valueOf(param);
			asyncResult.set(result);
		} catch (NumberFormatException e) {
			asyncResult.setException(e);
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
		eventloop.post(() ->
				multiplyAndPrintX("123")
						.whenComplete(($, throwable) -> System.out.println(throwable == null ? "Done" : throwable.toString())));
		eventloop.run();
	}

	@Test
	public void testSimpleResult() throws ExecutionException, InterruptedException {
		final CompletableFuture<Integer> future = SettableStage.immediateStage(41)
				.thenApply(integer -> integer + 1)
				.toCompletableFuture();
		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test(expected = RuntimeException.class)
	public void testError() throws Throwable {
		final CompletableFuture<Integer> future = SettableStage
				.<Integer>immediateFailedStage(new RuntimeException("Test"))
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
		final SettableStage<Integer> startStage = SettableStage.create();
		CompletionStage<Integer> powerOfTwo = startStage;
		for (int i = 0; i < 100_000; i++) {
			powerOfTwo = powerOfTwo.thenApplyAsync(integer -> (integer * 2) % 1000000007);
		}

		final CompletableFuture<Integer> future = powerOfTwo.toCompletableFuture();
		startStage.set(1);
		eventloop.run();
		future.get();
	}

	@Test(expected = RuntimeException.class)
	public void testLongStagesChainError() throws Throwable {
		final SettableStage<Integer> startStage = SettableStage.create();
		CompletionStage<Integer> powerOfTwo = startStage;
		for (int i = 0; i < 100_000; i++) {
			powerOfTwo = powerOfTwo.thenApplyAsync(integer -> (integer * 2) % 1000000007);
		}

		final CompletableFuture<Integer> future = powerOfTwo.toCompletableFuture();
		startStage.setException(new RuntimeException("Test"));
		eventloop.run();
		try {
			future.get();
		} catch (Exception e) {
			throw e.getCause();
		}
	}

	@Test
	public void testApplyWithExecutor() throws ExecutionException, InterruptedException {
		final CompletableFuture<Integer> future = SettableStage.immediateStage(2).thenApplyAsync(integer -> {
			assertFalse(eventloop.inEventloopThread());
			return 40 + integer;
		}, executor).toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test
	public void testAcceptWithExecutor() throws ExecutionException, InterruptedException {
		final AtomicInteger result = new AtomicInteger(0);
		final CompletableFuture<Void> future = SettableStage.immediateStage(null).thenAcceptAsync(integer -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(42);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testRunWithExecutor() throws ExecutionException, InterruptedException {
		final AtomicInteger result = new AtomicInteger(0);
		final CompletableFuture<Void> future = SettableStage.immediateStage(null).thenRunAsync(() -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(42);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testCombineWithExecutor() throws ExecutionException, InterruptedException {
		final SettableStage<Integer> stage1 = SettableStage.immediateStage(40);
		final SettableStage<Integer> stage2 = SettableStage.immediateStage(2);
		final CompletableFuture<Integer> future = stage1.thenCombineAsync(stage2, (integer, integer2) -> {
			assertFalse(eventloop.inEventloopThread());
			return integer + integer2;
		}, executor).toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test
	public void testAcceptBothWithExecutor() throws ExecutionException, InterruptedException {
		final AtomicInteger result = new AtomicInteger(0);
		final SettableStage<Integer> stage1 = SettableStage.immediateStage(2);
		final SettableStage<Integer> stage2 = SettableStage.immediateStage(40);
		final CompletableFuture<Void> future = stage1.thenAcceptBothAsync(stage2, (integer, integer2) -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(integer + integer2);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testRunAfterBothWithExecutor() throws ExecutionException, InterruptedException {
		final AtomicInteger result = new AtomicInteger(0);
		final SettableStage<Object> stage1 = SettableStage.immediateStage(null);
		final SettableStage<Object> stage2 = SettableStage.immediateStage(null);
		final CompletableFuture<Void> future = stage1.runAfterBothAsync(stage2, () -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(42);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testApplyToEitherWithExecutor() throws ExecutionException, InterruptedException {
		final SettableStage<Integer> stage1 = SettableStage.immediateStage(2);
		final SettableStage<Integer> stage2 = SettableStage.immediateStage(40);
		final CompletableFuture<Integer> future = stage1.applyToEitherAsync(stage2, integer -> {
			assertFalse(eventloop.inEventloopThread());
			return integer;
		}, executor).toCompletableFuture();

		eventloop.run();
		assertTrue(asList(2, 40).contains(future.get()));
	}

	@Test
	public void testAcceptEitherWithExecutor() throws ExecutionException, InterruptedException {
		final AtomicInteger result = new AtomicInteger(0);
		final SettableStage<Integer> stage1 = SettableStage.immediateStage(2);
		final SettableStage<Integer> stage2 = SettableStage.immediateStage(40);
		final CompletableFuture<Void> future = stage1.acceptEitherAsync(stage2, integer -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(integer);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertTrue(asList(2, 40).contains(result.get()));
	}

	@Test
	public void testRunAfterEitherWithExecutor() throws ExecutionException, InterruptedException {
		final AtomicInteger result = new AtomicInteger(0);
		final SettableStage<Integer> stage1 = SettableStage.immediateStage(2);
		final SettableStage<Integer> stage2 = SettableStage.immediateStage(40);
		final CompletableFuture<Void> future = stage1.runAfterEitherAsync(stage2, () -> {
			assertFalse(eventloop.inEventloopThread());
			assertTrue(stage1.isSet() || stage2.isSet());
			result.set(42);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testComposeWithExecutor() throws ExecutionException, InterruptedException {
		final CompletableFuture<Integer> future = SettableStage.immediateStage(2).thenComposeAsync(integer -> {
			assertFalse(eventloop.inEventloopThread());
			return SettableStage.immediateStage(40 + integer);
		}, executor).toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test
	public void testWhenCompleteWithExecutor() throws ExecutionException, InterruptedException {
		final AtomicInteger result = new AtomicInteger(0);
		final CompletableFuture<Integer> future = SettableStage.immediateStage(2).whenCompleteAsync((integer, throwable) -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(40 + integer);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testHandleWithExecutor() throws ExecutionException, InterruptedException {
		final SettableStage<Integer> stage1 = SettableStage.immediateStage(2);
		final CompletableFuture<Integer> future = stage1.handleAsync((integer, throwable) -> {
			assertFalse(eventloop.inEventloopThread());
			return 40 + integer;
		}, executor).toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

}