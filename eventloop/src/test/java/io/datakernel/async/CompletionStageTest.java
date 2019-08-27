package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.async.IndexedCollector.toList;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

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
		SettableStage<Integer> startStage = SettableStage.create();
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
		SettableStage<Integer> startStage = SettableStage.create();
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


	@Test
	public void testTheSameStage() {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		Stage<Integer> stage = Stage.of(10);
		Stage<List<Integer>> listStage = Stages.collect(asList(stage, stage, stage.thenApply(num -> num + 1)), toList());
		listStage.whenResult(res -> assertEquals(asList(10, 10, 11), res));
		eventloop.run();
	}

	@Test
	public void testSingleChain() {
		Stage<Integer> stage = Stage.of(10);
		for (int i = 0; i < 10; i++) {
			stage.thenApply(num -> num + 1);
		}
		stage.whenResult(res -> assertEquals(res.intValue(), 10));
		eventloop.run();
	}

	@Test
	public void testMultiChain() {
		Stage<Integer> stage = Stage.of(10).whenResult(res -> assertEquals(10, res.intValue()));
		for (int i = 0; i < 10; i++) {
			stage = stage.thenApply(num -> num + 1);
		}
		stage.whenResult(res -> assertEquals(20, res.intValue()));
		eventloop.run();
	}

	@Test
	public void testThenCompose() {
		Stage<Integer> mainStage = Stage.of(10);
		Stage.of(100).thenCompose(res -> mainStage.whenResult(Assert::assertNotNull));
		mainStage.whenComplete((res, e) -> assertNotNull(res));
		eventloop.run();
	}

	@Test
	public void testWhenResultChain() {
		Stage<Integer> mainStage = Stage.of(10);
		for (int i = 0; i < 10; i++) {
			Stage<Integer> newStage = Stage.of(10);
			mainStage.thenCompose(res -> newStage.whenResult(System.out::println));
			newStage.whenComplete((res, e) -> assertNotNull(res));
		}
		mainStage.whenComplete((res, e) -> assertNotNull(res));
		eventloop.run();
	}

	@Test
	public void testSubscribeCompletedStage() {
		Stage<Integer> newStage = Stage.of(10);
		Stage.of(10).thenCompose(res -> newStage.whenResult(Assert::assertNotNull));
		newStage.whenComplete((res, e) -> assertNotNull(res));
		eventloop.run();
	}

	@Test
	public void testWhenCompletedMethods() {
		Stage<Integer> stage = Stage.of(10);
		stage.whenResult(Assert::assertNotNull);
		stage.whenComplete((res, e) -> assertNotNull(res));
		stage.whenException(Assert::assertNull);
		eventloop.run();
	}

	@Test
	public void testSubscribeSelf() {
		Stage<Integer> stage = Stage.of(10);
		for (int i = 0; i < 10; i++) {
			stage.thenCompose(res -> stage);
		}
		stage.whenResult(Assert::assertNotNull);
		eventloop.run();
	}
}
