package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class CompletionStageTest {

	private Eventloop eventloop;
	private ExecutorService executor;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
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
		CompletableFuture<Integer> future = Stages.of(41)
				.thenApply(integer -> integer + 1)
				.toCompletableFuture();
		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test(expected = RuntimeException.class)
	public void testError() throws Throwable {
		CompletableFuture<Integer> future = Stages.<Integer>ofException(new RuntimeException("Test"))
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
		CompletionStage<Integer> powerOfTwo = startStage;
		for (int i = 0; i < 100_000; i++) {
			powerOfTwo = powerOfTwo.thenApplyAsync(integer -> (integer * 2) % 1000000007);
		}

		CompletableFuture<Integer> future = powerOfTwo.toCompletableFuture();
		startStage.set(1);
		eventloop.run();
		future.get();
	}

	@Test(expected = RuntimeException.class)
	public void testLongStagesChainError() throws Throwable {
		SettableStage<Integer> startStage = SettableStage.create();
		CompletionStage<Integer> powerOfTwo = startStage;
		for (int i = 0; i < 100_000; i++) {
			powerOfTwo = powerOfTwo.thenApplyAsync(integer -> (integer * 2) % 1000000007);
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

	@Test
	public void testThenApplyAsync() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> future = Stages.of(2)
				.thenApplyAsync(integer -> integer + 40)
				.toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test
	public void testApplyWithExecutor() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> future = Stages.of(2).thenApplyAsync(integer -> {
			assertFalse(eventloop.inEventloopThread());
			return 40 + integer;
		}, executor).toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test
	public void testThenAcceptAsync() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletableFuture<Void> future = Stages.of(5)
				.thenAcceptAsync(result::set)
				.toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(5, result.get());
	}

	@Test
	public void testAcceptWithExecutor() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletableFuture<Void> future = Stages.of(null).thenAcceptAsync(integer -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(42);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testThenRunAsync() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletableFuture<Void> future = Stages.of(null)
				.thenRunAsync(() -> result.set(42))
				.toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testRunWithExecutor() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletableFuture<Void> future = Stages.of(null).thenRunAsync(() -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(42);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testThenCombineAsync() throws ExecutionException, InterruptedException {
		CompletionStage<Integer> stage1 = Stages.of(40);
		CompletionStage<Integer> stage2 = Stages.of(2);

		CompletableFuture<Integer> future = stage1
				.thenCombineAsync(stage2, Integer::sum)
				.toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test
	public void testCombineWithExecutor() throws ExecutionException, InterruptedException {
		CompletionStage<Integer> stage1 = Stages.of(40);
		CompletionStage<Integer> stage2 = Stages.of(2);
		CompletableFuture<Integer> future = stage1.thenCombineAsync(stage2, (integer, integer2) -> {
			assertFalse(eventloop.inEventloopThread());
			return integer + integer2;
		}, executor).toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test
	public void testAcceptBothAsync() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletionStage<Integer> stage1 = Stages.of(2);
		CompletionStage<Integer> stage2 = Stages.of(40);
		CompletableFuture<Void> future = stage1
				.thenAcceptBothAsync(stage2, (integer, integer2) -> result.set(integer + integer2))
				.toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testAcceptBothWithExecutor() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletionStage<Integer> stage1 = Stages.of(2);
		CompletionStage<Integer> stage2 = Stages.of(40);
		CompletableFuture<Void> future = stage1.thenAcceptBothAsync(stage2, (integer, integer2) -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(integer + integer2);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testRunAfterBothAsync() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletionStage<Object> stage1 = Stages.of(null);
		CompletionStage<Object> stage2 = Stages.of(null);
		CompletableFuture<Void> future = stage1
				.runAfterBothAsync(stage2, () -> result.set(42))
				.toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testRunAfterBothWithExecutor() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletionStage<Object> stage1 = Stages.of(null);
		CompletionStage<Object> stage2 = Stages.of(null);
		CompletableFuture<Void> future = stage1.runAfterBothAsync(stage2, () -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(42);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testApplyToEitherAsync() throws ExecutionException, InterruptedException {
		CompletionStage<Integer> stage1 = Stages.of(2);
		CompletionStage<Integer> stage2 = Stages.of(40);

		CompletableFuture<Integer> future = stage1
				.applyToEitherAsync(stage2, integer -> integer)
				.toCompletableFuture();

		eventloop.run();
		assertTrue(asList(2, 40).contains(future.get()));
	}

	@Test
	public void testApplyToEitherWithExecutor() throws ExecutionException, InterruptedException {
		CompletionStage<Integer> stage1 = Stages.of(2);
		CompletionStage<Integer> stage2 = Stages.of(40);
		CompletableFuture<Integer> future = stage1.applyToEitherAsync(stage2, integer -> {
			assertFalse(eventloop.inEventloopThread());
			return integer;
		}, executor).toCompletableFuture();

		eventloop.run();
		assertTrue(asList(2, 40).contains(future.get()));
	}

	@Test
	public void testAcceptEitherAsync() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);

		CompletionStage<Integer> stage1 = Stages.of(2);
		CompletionStage<Integer> stage2 = Stages.of(40);

		CompletableFuture<Void> future = stage1
				.acceptEitherAsync(stage2, result::set)
				.toCompletableFuture();

		eventloop.run();
		future.get();
		assertTrue(asList(2, 40).contains(result.get()));
	}

	@Test
	public void testAcceptEitherWithExecutor() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletionStage<Integer> stage1 = Stages.of(2);
		CompletionStage<Integer> stage2 = Stages.of(40);
		CompletableFuture<Void> future = stage1.acceptEitherAsync(stage2, integer -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(integer);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertTrue(asList(2, 40).contains(result.get()));
	}

	@Test
	public void testRunAfterEitherAsync() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);

		CompletionStage<Integer> stage1 = Stages.of(2);
		CompletionStage<Integer> stage2 = Stages.of(40);

		CompletableFuture<Void> future = stage1
				.runAfterEitherAsync(stage2, () -> result.set(42))
				.toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testRunAfterEitherWithExecutor() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletionStage<Integer> stage1 = Stages.of(2);
		CompletionStage<Integer> stage2 = Stages.of(40);
		CompletableFuture<Void> future = stage1.runAfterEitherAsync(stage2, () -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(42);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testThenComposeAsync() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> future = Stages.of(2)
				.thenComposeAsync(integer -> Stages.of(40 + integer))
				.toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test
	public void testComposeWithExecutor() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> future = Stages.of(2).thenComposeAsync(integer -> {
			assertFalse(eventloop.inEventloopThread());
			return Stages.of(40 + integer);
		}, executor).toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test
	public void testWhenCompleteAsync() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletableFuture<Integer> future = Stages.of(2)
				.whenCompleteAsync((integer, throwable) -> result.set(40 + integer))
				.toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testWhenCompleteWithExecutor() throws ExecutionException, InterruptedException {
		AtomicInteger result = new AtomicInteger(0);
		CompletableFuture<Integer> future = Stages.of(2).whenCompleteAsync((integer, throwable) -> {
			assertFalse(eventloop.inEventloopThread());
			result.set(40 + integer);
		}, executor).toCompletableFuture();

		eventloop.run();
		future.get();
		assertEquals(42, result.get());
	}

	@Test
	public void testHandleAsync() throws ExecutionException, InterruptedException {
		CompletionStage<Integer> stage = Stages.of(2);
		CompletableFuture<Integer> future = stage.
				handleAsync((integer, throwable) -> 40 + integer)
				.toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

	@Test
	public void testHandleWithExecutor() throws ExecutionException, InterruptedException {
		CompletionStage<Integer> stage1 = Stages.of(2);
		CompletableFuture<Integer> future = stage1.handleAsync((integer, throwable) -> {
			assertFalse(eventloop.inEventloopThread());
			return 40 + integer;
		}, executor).toCompletableFuture();

		eventloop.run();
		assertEquals(42, ((int) future.get()));
	}

}