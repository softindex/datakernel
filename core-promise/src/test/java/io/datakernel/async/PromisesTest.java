/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import io.datakernel.util.*;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.async.Promise.of;
import static io.datakernel.async.Promise.ofException;
import static io.datakernel.async.Promises.*;
import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;

public final class PromisesTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	private final AtomicInteger counter = new AtomicInteger();

	@Test
	public void toListEmptyTest() {
		List<Integer> list = await(Promises.toList());
		assertEquals(0, list.size());
		// asserting immutability
		try {
			list.add(123);
		} catch (UnsupportedOperationException e) {
			return;
		}
		fail();
	}

	@Test
	public void toListSingleTest() {
		List<Integer> list = await(Promises.toList(of(321)));
		assertEquals(1, list.size());
	}

	@Test
	public void varargsToListTest() {
		List<Integer> list = await(Promises.toList(of(321), of(322), of(323)));
		assertEquals(3, list.size());
	}

	@Test
	public void streamToListTest() {
		List<Integer> list = await(Promises.toList(Stream.of(of(321), of(322), of(323))));
		assertEquals(3, list.size());
	}

	@Test
	public void listToListTest() {
		List<Integer> list = await(Promises.toList(asList(of(321), of(322), of(323))));
		assertEquals(3, list.size());
	}

	@Test
	public void toArrayEmptyTest() {
		Object[] array = await(toArray(Object.class));
		assertEquals(0, array.length);
	}

	@Test
	public void toArraySingleTest() {
		Integer[] array = await(toArray(Integer.class, of(321)));
		assertEquals(1, array.length);
		assertEquals(new Integer(321), array[0]);

	}

	@Test
	public void arraytoArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, of(321), of(322)));
		assertEquals(2, array.length);
		assertEquals(new Integer(321), array[0]);
		assertEquals(new Integer(322), array[1]);
	}

	@Test
	public void varargsToArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, of(321), of(322), of(323)));
		assertEquals(3, array.length);
		assertEquals(new Integer(321), array[0]);
		assertEquals(new Integer(322), array[1]);
		assertEquals(new Integer(323), array[2]);

	}

	@Test
	public void streamToArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, Stream.of(of(321), of(322), of(323))));
		assertEquals(3, array.length);
		assertEquals(new Integer(321), array[0]);
		assertEquals(new Integer(322), array[1]);
		assertEquals(new Integer(323), array[2]);
	}

	@Test
	public void listToArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, asList(of(321), of(322), of(323))));
		assertEquals(3, array.length);
		assertEquals(new Integer(321), array[0]);
		assertEquals(new Integer(322), array[1]);
		assertEquals(new Integer(323), array[2]);
	}

	@Test
	public void toTuple1Test() {
		Tuple1<Integer> tuple1 = await(toTuple(Tuple1::new, of(321)));
		assertEquals(new Integer(321), tuple1.getValue1());

		Tuple1<Integer> tuple2 = await(toTuple(of(321)));
		assertEquals(new Integer(321), tuple2.getValue1());
	}

	@Test
	public void toTuple2Test() {
		Tuple2<Integer, String> tuple1 = await(toTuple(Tuple2::new, of(321), of("322")));
		assertEquals(new Integer(321), tuple1.getValue1());
		assertEquals("322", tuple1.getValue2());

		Tuple2<Integer, String> tuple2 = await(toTuple(of(321), of("322")));
		assertEquals(new Integer(321), tuple2.getValue1());
		assertEquals("322", tuple2.getValue2());
	}

	@Test
	public void toTuple3Test() {
		Tuple3<Integer, String, Double> tuple1 = await(toTuple(Tuple3::new, of(321), of("322"), of(323.34)));
		assertEquals(new Integer(321), tuple1.getValue1());
		assertEquals("322", tuple1.getValue2());
		assertEquals(323.34, tuple1.getValue3());

		Tuple3<Integer, String, Double> tuple2 = await(toTuple(of(321), of("322"), of(323.34)));
		assertEquals(new Integer(321), tuple2.getValue1());
		assertEquals("322", tuple2.getValue2());
		assertEquals(323.34, tuple2.getValue3());
	}

	@Test
	public void toTuple4Test() {
		Tuple4<Integer, String, Double, Duration> tuple1 = await(toTuple(Tuple4::new, of(321), of("322"), of(323.34), of(ofMillis(324))));
		assertEquals(new Integer(321), tuple1.getValue1());
		assertEquals("322", tuple1.getValue2());
		assertEquals(323.34, tuple1.getValue3());
		assertEquals(ofMillis(324), tuple1.getValue4());

		Tuple4<Integer, String, Double, Duration> tuple2 = await(toTuple(of(321), of("322"), of(323.34), of(ofMillis(324))));
		assertEquals(new Integer(321), tuple2.getValue1());
		assertEquals("322", tuple2.getValue2());
		assertEquals(323.34, tuple2.getValue3());
		assertEquals(ofMillis(324), tuple2.getValue4());
	}

	@Test
	public void toTuple5Test() {
		Tuple5<Integer, String, Double, Duration, Integer> tuple1 = await(toTuple(Tuple5::new, of(321), of("322"), of(323.34), of(ofMillis(324)), of(1)));
		assertEquals(new Integer(321), tuple1.getValue1());
		assertEquals("322", tuple1.getValue2());
		assertEquals(323.34, tuple1.getValue3());
		assertEquals(ofMillis(324), tuple1.getValue4());
		assertEquals(new Integer(1), tuple1.getValue5());

		Tuple5<Integer, String, Double, Duration, Integer> tuple2 = await(toTuple(of(321), of("322"), of(323.34), of(ofMillis(324)), of(1)));
		assertEquals(new Integer(321), tuple2.getValue1());
		assertEquals("322", tuple2.getValue2());
		assertEquals(323.34, tuple2.getValue3());
		assertEquals(ofMillis(324), tuple2.getValue4());
		assertEquals(new Integer(1), tuple2.getValue5());
	}

	@Test
	public void toTuple6Test() {
		Tuple6<Integer, String, Double, Duration, Integer, Object> tuple1 = await(toTuple(Tuple6::new, of(321), of("322"), of(323.34), of(ofMillis(324)), of(1), of(null)));
		assertEquals(new Integer(321), tuple1.getValue1());
		assertEquals("322", tuple1.getValue2());
		assertEquals(323.34, tuple1.getValue3());
		assertEquals(ofMillis(324), tuple1.getValue4());
		assertEquals(new Integer(1), tuple1.getValue5());
		assertNull(tuple1.getValue6());

		Tuple6<Integer, String, Double, Duration, Integer, Object> tuple2 = await(toTuple(of(321), of("322"), of(323.34), of(ofMillis(324)), of(1), of(null)));
		assertEquals(new Integer(321), tuple2.getValue1());
		assertEquals("322", tuple2.getValue2());
		assertEquals(323.34, tuple2.getValue3());
		assertEquals(ofMillis(324), tuple2.getValue4());
		assertEquals(new Integer(1), tuple2.getValue5());
		assertNull(tuple2.getValue6());
	}

	@Test
	public void testCollectStream() {
		List<Integer> list = await(Promises.toList(Stream.of(of(1), of(2), of(3))));
		assertEquals(3, list.size());
	}

	@Test
	public void testRepeat() {
		Exception exception = new Exception();
		Throwable e = awaitException(repeat(() -> {
			if (counter.get() == 5) {
				return Promise.ofException(exception);
			}
			counter.incrementAndGet();
			return Promise.of(null);
		}));
		System.out.println(counter);
		assertSame(exception, e);
		assertEquals(5, counter.get());
	}

	@Test
	public void testLoop() {
		Promises.loop(0, AsyncPredicate.of(i -> i < 5), i -> Promise.of(i + 1)
				.whenResult(counter::set));
		assertEquals(5, counter.get());
	}

	@Test
	public void testLoopAsync() {
		await(Promises.loop(0, AsyncPredicate.of(i -> i < 5), i ->
				Promises.delay(Promise.of(i + 1), 10)
						.whenResult(counter::set)
						.whenResult(System.out::println)));
		assertEquals(5, counter.get());
	}

	@Test
	public void testRunSequence() {
		List<Integer> list = asList(1, 2, 3, 4, 5, 6, 7);
		await(sequence(list.stream()
				.map(n ->
						() -> getStage(n).toVoid())));
	}

	@Test
	public void testRunSequenceWithSorted() {
		List<Integer> list = asList(1, 2, 3, 4, 5, 6, 7);
		await(sequence(list.stream()
				.sorted(Comparator.naturalOrder())
				.map(n ->
						() -> getStage(n).toVoid())));
	}

	@SuppressWarnings("all")
	@Test
	public void testSomeMethodWithZeroParam() {
		Promise<?> some = some(100);
		assertEquals(some.getClass(), CompleteExceptionallyPromise.class);
	}

	@SuppressWarnings("all")
	@Test
	public void testSomeMethodWithOneParamAndGetOne() {
		Integer result = 100;
		Promise<List<Integer>> some = some(Promise.of(result), 1);
		Integer gotResult = some.materialize().getResult().get(0);
		assertEquals(result, gotResult);
	}

	@SuppressWarnings("all")
	@Test(expected = IndexOutOfBoundsException.class)
	public void testSomeMethodWithOneParamAndGetNone() {
		Integer value = 100;
		Promise<List<Integer>> some = some(Promise.of(value), 0);
		Integer gotResult = some.materialize().getResult().get(0);
	}

	@SuppressWarnings("all")
	@Test
	public void testSomeMethodWithTwoParamAndGetTwo() {
		Integer result1 = 100;
		Integer result2 = 101;
		Promise<List<Integer>> resultPromise = some(Promise.of(result1), Promise.of(result2), 2);

		resultPromise.whenResult(list -> assertEquals(2, list.size()))
				.whenResult(list -> {
					Integer gotResult1 = list.get(0);
					Integer gotResult2 = list.get(1);
					assertTrue(result1 == gotResult1 || result1 == gotResult2);
					assertTrue(result2 == gotResult1 || result2 == gotResult2);
				});
	}

	@SuppressWarnings("all")
	@Test
	public void testSomeMethodWithTwoParamAndGetOne() {
		Integer result = 100;
		Promise<List<Integer>> some = some(Promise.of(result), Promise.of(result), 1);
		List<Integer> list = some.materialize().getResult();

		assertEquals(1, list.size());
		Integer gotResult1 = list.get(0);
		assertTrue(result == gotResult1);
	}

	@SuppressWarnings("all")
	@Test
	public void testSomeWithManyParamsAndGetAhalfOfThem() {
		List<Promise<Integer>> params = Stream.generate(() -> of(0)).limit(10).collect(Collectors.toList());

		Promise<List<Integer>> promiseResult = some(params, params.size() / 2);
		promiseResult.whenResult(result -> assertEquals(params.size() / 2, result.size()));
	}

	@SuppressWarnings("all")
	@Test
	public void testSomeWithManyParamsAndGetNone() {
		List<Promise<Integer>> params = Stream.generate(() -> of(0)).limit(10).collect(Collectors.toList());

		Promise<List<Integer>> promiseResult = some(params, 0);
		promiseResult.whenResult(result -> assertEquals(0, result.size()));
	}

	@SuppressWarnings("all")
	@Test
	public void testSomeWithManyParamsWithDelayAndGetAhalfOfThem() {
		List<Promise<Integer>> params = Stream.generate(() -> delay(of(0), 1000)).limit(10)
				.collect(Collectors.toList());

		Promise<List<Integer>> promiseResult = some(params, params.size() / 2);
		promiseResult.whenResult(result -> assertEquals(params.size() / 2, result.size()));
	}


	@SuppressWarnings("all")
	@Test
	public void testSomeTheWholeAreFailed() {
		List<CompleteExceptionallyPromise<Object>> params = Stream.
				generate(() -> ofException(new RuntimeException()))
				.limit(10)
				.collect(Collectors.toList());

		Promise<List<Object>> result = some(params, params.size() / 2);
		assertTrue(result.isException());
	}

	@Test
	public void testSomeNotEnoughCompleteResult() {
		List<Promise<?>> params = asList(of(10),
				delay(ofException(new RuntimeException()), 10000),
				of(100),
				ofException(new RuntimeException()));

		Promise<List<Object>> result = some(params, 3);
		result.whenException(e -> assertTrue(true));
	}

	private Promise<Integer> getStage(Integer number) {
		assertEquals(0, counter.get());
		counter.incrementAndGet();
		SettablePromise<Integer> promise = new SettablePromise<>();
		Eventloop.getCurrentEventloop().post(() -> promise.set(number));
		return promise
				.then(n -> {
					counter.decrementAndGet();
					return Promise.of(n);
				});
	}
}
