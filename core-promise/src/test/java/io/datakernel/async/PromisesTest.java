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

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.datakernel.async.Promise.*;
import static io.datakernel.async.Promises.*;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static java.lang.System.out;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static junit.framework.TestCase.*;

@RunWith(DatakernelRunner.class)
public final class PromisesTest {
	private final AtomicInteger counter = new AtomicInteger();

	@Test
	public void toListEmptyTest() {
		Promises.toList()
				.whenComplete(assertComplete(list -> {
					assertEquals(0, list.size());
					// asserting immutability
					try {
						list.add(123);
					} catch (UnsupportedOperationException e) {
						return;
					}
					fail();
				}));
	}

	@Test
	public void toListSingleTest() {
		Promises.toList(of(321))
				.whenComplete(assertComplete(list -> {
					assertEquals(1, list.size());

					// asserting mutability
					Integer newInt = 123;
					list.set(0, newInt);
					assertEquals(newInt, list.get(0));

				}));
	}

	@Test
	public void varargsToListTest() {
		Promises.toList(of(321), of(322), of(323))
				.whenComplete(assertComplete(list -> {
					assertEquals(3, list.size());

					// asserting mutability
					list.set(0, 123);
					assertEquals(3, list.size());
					assertEquals(new Integer(123), list.get(0));

				}));
	}

	@Test
	public void streamToListTest() {
		Promises.toList(Stream.of(of(321), of(322), of(323)))
				.whenComplete(assertComplete(list -> {
					assertEquals(3, list.size());

					// asserting mutability
					list.set(0, 123);
					assertEquals(3, list.size());
					assertEquals(new Integer(123), list.get(0));

				}));
	}

	@Test
	public void listToListTest() {
		Promises.toList(asList(of(321), of(322), of(323)))
				.whenComplete(assertComplete(list -> {
					assertEquals(3, list.size());

					// asserting mutability
					list.set(0, 123);
					assertEquals(3, list.size());
					assertEquals(new Integer(123), list.get(0));

				}));
	}

	@Test
	public void toArrayEmptyTest() {
		toArray(Object.class)
				.whenComplete(assertComplete(array -> assertEquals(0, array.length)));
	}

	@Test
	public void toArraySingleTest() {
		toArray(Integer.class, of(321))
				.whenComplete(assertComplete(array -> {
					assertEquals(1, array.length);
					assertEquals(new Integer(321), array[0]);
				}));
	}

	@Test
	public void arraytoArrayDoubleTest() {
		toArray(Integer.class, of(321), of(322))
				.whenComplete(assertComplete(array -> {
					assertEquals(2, array.length);
					assertEquals(new Integer(321), array[0]);
					assertEquals(new Integer(322), array[1]);
				}));
	}

	@Test
	public void varargsToArrayDoubleTest() {
		toArray(Integer.class, of(321), of(322), of(323))
				.whenComplete(assertComplete(array -> {
					assertEquals(3, array.length);
					assertEquals(new Integer(321), array[0]);
					assertEquals(new Integer(322), array[1]);
					assertEquals(new Integer(323), array[2]);
				}));
	}

	@Test
	public void streamToArrayDoubleTest() {
		toArray(Integer.class, Stream.of(of(321), of(322), of(323)))
				.whenComplete(assertComplete(array -> {
					assertEquals(3, array.length);
					assertEquals(new Integer(321), array[0]);
					assertEquals(new Integer(322), array[1]);
					assertEquals(new Integer(323), array[2]);
				}));
	}

	@Test
	public void listToArrayDoubleTest() {
		toArray(Integer.class, asList(of(321), of(322), of(323)))
				.whenComplete(assertComplete(array -> {
					assertEquals(3, array.length);
					assertEquals(new Integer(321), array[0]);
					assertEquals(new Integer(322), array[1]);
					assertEquals(new Integer(323), array[2]);
				}));
	}

	@Test
	public void toTuple1Test() {
		toTuple(Tuple1::new, of(321))
				.whenComplete(assertComplete(value -> assertEquals(new Integer(321), value.getValue1())))
				.thenCompose($ -> toTuple(of(321))
						.whenComplete(assertComplete(value -> assertEquals(new Integer(321), value.getValue1()))));
	}

	@Test
	public void toTuple2Test() {
		toTuple(Tuple2::new, of(321), of("322"))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
				}))
				.thenCompose($ -> toTuple(of(321), of("322"))
						.whenComplete(assertComplete(value -> {
							assertEquals(new Integer(321), value.getValue1());
							assertEquals("322", value.getValue2());
						})));
	}

	@Test
	public void toTuple3Test() {
		toTuple(Tuple3::new, of(321), of("322"), of(323.34))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
				}))
				.thenCompose($ -> toTuple(of(321), of("322"), of(323.34))
						.whenComplete(assertComplete(value -> {
							assertEquals(new Integer(321), value.getValue1());
							assertEquals("322", value.getValue2());
							assertEquals(323.34, value.getValue3());
						})));
	}

	@Test
	public void toTuple4Test() {
		toTuple(Tuple4::new, of(321), of("322"), of(323.34), of(ofMillis(324)))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
					assertEquals(ofMillis(324), value.getValue4());
				}))
				.thenCompose($ -> toTuple(of(321), of("322"), of(323.34), of(ofMillis(324)))
						.whenComplete(assertComplete(value -> {
							assertEquals(new Integer(321), value.getValue1());
							assertEquals("322", value.getValue2());
							assertEquals(323.34, value.getValue3());
							assertEquals(ofMillis(324), value.getValue4());
						})));
	}

	@Test
	public void toTuple5Test() {
		toTuple(Tuple5::new, of(321), of("322"), of(323.34), of(ofMillis(324)), of(1))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
					assertEquals(ofMillis(324), value.getValue4());
					assertEquals(new Integer(1), value.getValue5());
				}))
				.thenCompose($ -> toTuple(of(321), of("322"), of(323.34), of(ofMillis(324)), of(1)))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
					assertEquals(ofMillis(324), value.getValue4());
					assertEquals(new Integer(1), value.getValue5());
				}));
	}

	@Test
	public void toTuple6Test() {
		toTuple(Tuple6::new, of(321), of("322"), of(323.34), of(ofMillis(324)), of(1), of(null))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
					assertEquals(ofMillis(324), value.getValue4());
					assertEquals(new Integer(1), value.getValue5());
					assertNull(value.getValue6());
				}))
				.thenCompose($ -> toTuple(of(321), of("322"), of(323.34), of(ofMillis(324)), of(1), of(null)))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
					assertEquals(ofMillis(324), value.getValue4());
					assertEquals(new Integer(1), value.getValue5());
					assertNull(value.getValue6());
				}));
	}

	@Test
	public void testCollectWithListener() {
		CollectListener<Object, Object[], List<Object>> any = (canceller, accumulator) -> {};
		collect(IndexedCollector.toList(), any, asList(of(1), of(2), of(3)))
				.whenComplete(assertComplete(list -> assertEquals(3, list.size())));
	}

	@Test
	public void testCollectStream() {
		collect(toList(), Stream.of(of(1), of(2), of(3)))
				.whenComplete(assertComplete(list -> assertEquals(3, list.size())));
	}

	@Test
	public void testRepeat() {
		AtomicInteger counter = new AtomicInteger(0);
		repeat(() -> complete()
				.thenCompose($ -> {
					if (counter.get() == 5) {
						return ofException(new Exception());
					}
					counter.incrementAndGet();
					return of($);
				})
				.whenResult($ -> out.println(counter)))
				.whenComplete(assertFailure(Exception.class))
				.whenException(e -> assertEquals(5, counter.get()));
	}

	@Test
	public void testRunSequence() {
		List<Integer> list = asList(1, 2, 3, 4, 5, 6, 7);
		runSequence(list.stream()
				.map(n -> AsyncSupplier.cast(() ->
						getStage(n))));
	}

	@Test
	public void testRunSequenceWithSorted() {
		List<Integer> list = asList(1, 2, 3, 4, 5, 6, 7);
		runSequence(list.stream()
				.sorted(Comparator.naturalOrder())
				.map(n -> AsyncSupplier.cast(() ->
						getStage(n))));
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
