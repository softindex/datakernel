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
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.util.*;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.datakernel.async.Promise.of;
import static io.datakernel.async.Promises.*;
import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(DatakernelRunner.class)
public final class PromisesTest {
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
		List<Integer> list = await(collect(toList(), Stream.of(of(1), of(2), of(3))));
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
		Promises.loop(0, i -> i < 5, i -> Promise.of(i+1)
				.whenResult(counter::set));
		assertEquals(5, counter.get());
	}

	@Test
	public void testLoopAsync() {
		await(Promises.loop(0, i -> i < 5, i ->
				Promises.delay(Promise.of(i + 1), 10)
						.whenResult(counter::set)
						.whenResult(System.out::println)));
		assertEquals(5, counter.get());
	}

	@Test
	public void testRunSequence() {
		List<Integer> list = asList(1, 2, 3, 4, 5, 6, 7);
		await(runSequence(list.stream()
				.map(n -> AsyncSupplier.cast(() ->
						getStage(n)))));
	}

	@Test
	public void testRunSequenceWithSorted() {
		List<Integer> list = asList(1, 2, 3, 4, 5, 6, 7);
		await(runSequence(list.stream()
				.sorted(Comparator.naturalOrder())
				.map(n -> AsyncSupplier.cast(() ->
						getStage(n)))));
	}

	private Promise<Integer> getStage(Integer number) {
		assertEquals(0, counter.get());
		counter.incrementAndGet();
		SettablePromise<Integer> promise = new SettablePromise<>();
		Eventloop.getCurrentEventloop().post(() -> promise.set(number));
		return promise
				.thenCompose(n -> {
					counter.decrementAndGet();
					return Promise.of(n);
				});
	}
}
