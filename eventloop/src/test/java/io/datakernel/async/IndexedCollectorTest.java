package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.async.IndexedCollector.toList;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class IndexedCollectorTest {
	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	}

	@Test
	public void testToArray() {
		IndexedCollector<Integer, Integer[], Integer[]> collector = IndexedCollector.toArray(Integer.class);
		Integer[] integers = collector.resultOf();
		assertEquals(0, integers.length);

		integers = collector.resultOf(100);
		assertEquals(1, integers.length);
		assertEquals(new Integer(100), integers[0]);

		integers = collector.resultOf(100, 200);
		assertEquals(2, integers.length);
		assertEquals(new Integer(100), integers[0]);
		assertEquals(new Integer(200), integers[1]);

		integers = collector.resultOf(asList(1, 2, 3, 4, 5, 6));
		assertEquals(6, integers.length);
		assertEquals(new Integer(1), integers[0]);
		assertEquals(new Integer(2), integers[1]);
		assertEquals(new Integer(3), integers[2]);
		assertEquals(new Integer(4), integers[3]);
		assertEquals(new Integer(5), integers[4]);
		assertEquals(new Integer(6), integers[5]);
	}

	@Test
	public void testToList() {
		IndexedCollector<Object, Object[], List<Object>> collector = toList();
		List<Object> integers = collector.resultOf();
		assertEquals(0, integers.size());

		integers = collector.resultOf(100);
		assertEquals(1, integers.size());
		assertEquals(100, integers.get(0));

		integers = collector.resultOf(100, 200);
		assertEquals(2, integers.size());
		assertEquals(100, integers.get(0));
		assertEquals(200, integers.get(1));

		integers = collector.resultOf(asList(1, 2, 3, 4, 5, 6));
		assertEquals(6, integers.size());
		assertEquals(1, integers.get(0));
		assertEquals(2, integers.get(1));
		assertEquals(3, integers.get(2));
		assertEquals(4, integers.get(3));
		assertEquals(5, integers.get(4));
		assertEquals(6, integers.get(5));
	}

	@Test
	public void testOfCollector() {
		IndexedCollector<Integer, ?, List<Integer>> collector = IndexedCollector.ofCollector(Collectors.toList());
		List<Integer> integers = collector.resultOf();
		assertEquals(0, integers.size());

		integers = collector.resultOf(100);
		assertEquals(1, integers.size());
		assertEquals(new Integer(100), integers.get(0));

		integers = collector.resultOf(100, 200);
		assertEquals(2, integers.size());
		assertEquals(new Integer(100), integers.get(0));
		assertEquals(new Integer(200), integers.get(1));

		integers = collector.resultOf(asList(1, 2, 3, 4, 5, 6));
		assertEquals(6, integers.size());
		assertEquals(new Integer(1), integers.get(0));
		assertEquals(new Integer(2), integers.get(1));
		assertEquals(new Integer(3), integers.get(2));
		assertEquals(new Integer(4), integers.get(3));
		assertEquals(new Integer(5), integers.get(4));
		assertEquals(new Integer(6), integers.get(5));
	}

	@Test
	public void testExceptionCollector() {
		Stage<Integer> stage = Stages.any(Stage.ofException(new RuntimeException()));
		Stage<List<Integer>> collect = Stages.collect(asList(stage, Stage.of(100)), toList());
		collect.whenComplete((list, e) -> assertTrue(e instanceof RuntimeException));
		eventloop.run();
	}

	@Test
	public void testIndexedCollectorWithCompletedStages() {
		Stage<Integer> first = Stage.of(10).whenComplete((res, e) -> assertNotNull(res));
		Stage<Integer> second = Stage.of(100).thenCompose((res) -> first.whenComplete((newRes, newE) -> assertNotNull(newRes)));
		Stage<List<Integer>> collectedStage = Stages.collect(asList(first, second), toList());
		collectedStage.whenComplete((res, e) -> assertEquals(asList(10, 10), res));
		eventloop.run();
	}

	@Test
	public void testCollectVoid() {
		Stage<List<Void>> voidCollectedStages = Stages.collect(
				asList(Stage.of(10)
								.whenResult(Assert::assertNotNull).toVoid(),
						Stage.of(100)
								.whenResult(Assert::assertNotNull).toVoid()),
				IndexedCollector.toList());
		voidCollectedStages.whenResult(res -> assertEquals(asList(null, null), res));
		eventloop.run();
	}

	@Test
	public void testCollectArray() {
		Stage<Integer> first = Stage.of(10);
		List<Stage<Integer>> stages = new ArrayList<>();
		stages.add(first);
		for (int i = 1; i < 3; i++) {
			int finalI = i;
			stages.add(first.thenApply(res -> res + finalI));
		}
		Stage<Integer[]> collected = Stages.collect(stages, IndexedCollector.toArray(Integer.class));
		collected.whenResult(res -> assertEquals(res, new Integer[]{10, 11, 12}));
		eventloop.run();
	}

	@Test
	public void testCollectorListenerTimeout() {
		List<Stage<Integer>> collected = Stream.generate(() -> Stage.of(10)).limit(10).collect(Collectors.toList());
		Stage<List<Integer>> collectedStageList = Stages.collect(collected, CollectListener.timeout(100), toList());
		collectedStageList.whenResult(res -> assertEquals(res, Stream.generate(() -> 10).limit(10).collect(Collectors.toList())));
		eventloop.run();
	}

	@Test
	public void testCollectorListenerAny() {
		List<Stage<Integer>> collected = Stream.generate(() -> Stage.of(10)).limit(10).collect(Collectors.toList());
		Stage<List<Integer>> collectedStageList = Stages.collect(collected, CollectListener.any(3), toList());
		collectedStageList.whenResult(res -> assertEquals(3, res.stream().filter(Objects::nonNull).count()));
		eventloop.run();
	}

	@Test
	public void testCollectorListenerAnyAndOneException() {
		List<Stage<Integer>> collected = Stream.generate(() -> Stage.of(10)).limit(10).collect(Collectors.toList());
		collected.add(Stage.ofCallable(Executors.newSingleThreadExecutor(), () -> {
			Thread.sleep(500);
			throw new Exception();
		}));
		Stage<List<Integer>> collectedStageList = Stages.collect(collected, CollectListener.any(1), toList());
		collectedStageList.whenComplete((res, e) -> assertNull(e));
		eventloop.run();
	}
}
