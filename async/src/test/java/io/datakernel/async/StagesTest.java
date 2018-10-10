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
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.stream.processor.ActiveStagesRule;
import io.datakernel.util.*;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import static io.datakernel.test.TestUtils.assertComplete;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static junit.framework.TestCase.*;

public class StagesTest {
	@Rule
	public ActiveStagesRule activeStagesRule = new ActiveStagesRule();

	private final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError()).withCurrentThread();

	@Test
	public void toListEmptyTest() {
		eventloop.post(() -> Stages.toList()
				.whenComplete(assertComplete(list -> {
					assertEquals(0, list.size());
					// asserting immutability
					try {
						list.add(123);
					} catch (UnsupportedOperationException e) {
						return;
					}
					fail();
				})));
		eventloop.run();
	}

	@Test
	public void toListSingleTest() {
		eventloop.post(() -> Stages.toList(Stage.of(321))
				.whenComplete(assertComplete(list -> {
					assertEquals(1, list.size());

					// asserting mutability
					Integer newInt = 123;
					list.set(0, newInt);
					assertEquals(newInt, list.get(0));

				})));
		eventloop.run();
	}

	@Test
	public void varargsToListTest() {
		eventloop.post(() -> Stages.toList(Stage.of(321), Stage.of(322), Stage.of(323))
				.whenComplete(assertComplete(list -> {
					assertEquals(3, list.size());

					// asserting mutability
					list.set(0, 123);
					assertEquals(3, list.size());
					assertEquals(new Integer(123), list.get(0));

				})));
		eventloop.run();
	}

	@Test
	public void streamToListTest() {
		eventloop.post(() -> Stages.toList(Stream.of(Stage.of(321), Stage.of(322), Stage.of(323)))
				.whenComplete(assertComplete(list -> {
					assertEquals(3, list.size());

					// asserting mutability
					list.set(0, 123);
					assertEquals(3, list.size());
					assertEquals(new Integer(123), list.get(0));

				})));
		eventloop.run();
	}

	@Test
	public void listToListTest() {
		eventloop.post(() -> Stages.toList(asList(Stage.of(321), Stage.of(322), Stage.of(323)))
				.whenComplete(assertComplete(list -> {
					assertEquals(3, list.size());

					// asserting mutability
					list.set(0, 123);
					assertEquals(3, list.size());
					assertEquals(new Integer(123), list.get(0));

				})));
		eventloop.run();
	}

	@Test
	public void toArrayEmptyTest() {
		eventloop.post(() -> Stages.toArray(Object.class)
				.whenComplete(assertComplete(array -> assertEquals(0, array.length))));
		eventloop.run();
	}

	@Test
	public void toArraySingleTest() {
		eventloop.post(() -> Stages.toArray(Integer.class, Stage.of(321))
				.whenComplete(assertComplete(array -> {
					assertEquals(1, array.length);
					assertEquals(new Integer(321), array[0]);
				})));
		eventloop.run();
	}

	@Test
	public void arraytoArrayDoubleTest() {
		eventloop.post(() -> Stages.toArray(Integer.class, Stage.of(321), Stage.of(322))
				.whenComplete(assertComplete(array -> {
					assertEquals(2, array.length);
					assertEquals(new Integer(321), array[0]);
					assertEquals(new Integer(322), array[1]);
				})));
		eventloop.run();
	}

	@Test
	public void varargsToArrayDoubleTest() {
		eventloop.post(() -> Stages.toArray(Integer.class, Stage.of(321), Stage.of(322), Stage.of(323))
				.whenComplete(assertComplete(array -> {
					assertEquals(3, array.length);
					assertEquals(new Integer(321), array[0]);
					assertEquals(new Integer(322), array[1]);
					assertEquals(new Integer(323), array[2]);
				})));
		eventloop.run();
	}

	@Test
	public void streamToArrayDoubleTest() {
		eventloop.post(() -> Stages.toArray(Integer.class, Stream.of(Stage.of(321), Stage.of(322), Stage.of(323)))
				.whenComplete(assertComplete(array -> {
					assertEquals(3, array.length);
					assertEquals(new Integer(321), array[0]);
					assertEquals(new Integer(322), array[1]);
					assertEquals(new Integer(323), array[2]);
				})));
		eventloop.run();
	}

	@Test
	public void listToArrayDoubleTest() {
		eventloop.post(() -> Stages.toArray(Integer.class, asList(Stage.of(321), Stage.of(322), Stage.of(323)))
				.whenComplete(assertComplete(array -> {
					assertEquals(3, array.length);
					assertEquals(new Integer(321), array[0]);
					assertEquals(new Integer(322), array[1]);
					assertEquals(new Integer(323), array[2]);
				})));
		eventloop.run();
	}

	@Test
	public void toTuple1Test() {
		eventloop.post(() -> Stages.toTuple(Tuple1::new, Stage.of(321))
				.whenComplete(assertComplete(value -> assertEquals(new Integer(321), value.getValue1())))
				.thenCompose($ -> Stages.toTuple(Stage.of(321))
						.whenComplete(assertComplete(value -> assertEquals(new Integer(321), value.getValue1())))));
		eventloop.run();
	}

	@Test
	public void toTuple2Test() {
		eventloop.post(() -> Stages.toTuple(Tuple2::new, Stage.of(321), Stage.of("322"))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
				}))
				.thenCompose($ -> Stages.toTuple(Stage.of(321), Stage.of("322"))
						.whenComplete(assertComplete(value -> {
							assertEquals(new Integer(321), value.getValue1());
							assertEquals("322", value.getValue2());
						}))));
		eventloop.run();
	}

	@Test
	public void toTuple3Test() {
		eventloop.post(() -> Stages.toTuple(Tuple3::new, Stage.of(321), Stage.of("322"), Stage.of(323.34))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
				}))
				.thenCompose($ -> Stages.toTuple(Stage.of(321), Stage.of("322"), Stage.of(323.34))
						.whenComplete(assertComplete(value -> {
							assertEquals(new Integer(321), value.getValue1());
							assertEquals("322", value.getValue2());
							assertEquals(323.34, value.getValue3());
						}))));
		eventloop.run();
	}

	@Test
	public void toTuple4Test() {
		eventloop.post(() -> Stages.toTuple(Tuple4::new, Stage.of(321), Stage.of("322"), Stage.of(323.34), Stage.of(Duration.ofMillis(324)))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
					assertEquals(Duration.ofMillis(324), value.getValue4());
				}))
				.thenCompose($ -> Stages.toTuple(Stage.of(321), Stage.of("322"), Stage.of(323.34), Stage.of(Duration.ofMillis(324)))
						.whenComplete(assertComplete(value -> {
							assertEquals(new Integer(321), value.getValue1());
							assertEquals("322", value.getValue2());
							assertEquals(323.34, value.getValue3());
							assertEquals(Duration.ofMillis(324), value.getValue4());
						}))));
		eventloop.run();
	}

	@Test
	public void toTuple5Test() {
		eventloop.post(() -> Stages.toTuple(Tuple5::new, Stage.of(321), Stage.of("322"), Stage.of(323.34), Stage.of(Duration.ofMillis(324)), Stage.of(1))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
					assertEquals(Duration.ofMillis(324), value.getValue4());
					assertEquals(new Integer(1), value.getValue5());
				}))
				.thenCompose($ -> Stages.toTuple(Stage.of(321), Stage.of("322"), Stage.of(323.34), Stage.of(Duration.ofMillis(324)), Stage.of(1)))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
					assertEquals(Duration.ofMillis(324), value.getValue4());
					assertEquals(new Integer(1), value.getValue5());
				})));
		eventloop.run();
	}

	@Test
	public void toTuple6Test() {
		eventloop.post(() -> Stages.toTuple(Tuple6::new, Stage.of(321), Stage.of("322"), Stage.of(323.34), Stage.of(Duration.ofMillis(324)), Stage.of(1), Stage.of(null))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
					assertEquals(Duration.ofMillis(324), value.getValue4());
					assertEquals(new Integer(1), value.getValue5());
					assertNull(value.getValue6());
				}))
				.thenCompose($ -> Stages.toTuple(Stage.of(321), Stage.of("322"), Stage.of(323.34), Stage.of(Duration.ofMillis(324)), Stage.of(1), Stage.of(null)))
				.whenComplete(assertComplete(value -> {
					assertEquals(new Integer(321), value.getValue1());
					assertEquals("322", value.getValue2());
					assertEquals(323.34, value.getValue3());
					assertEquals(Duration.ofMillis(324), value.getValue4());
					assertEquals(new Integer(1), value.getValue5());
					assertNull(value.getValue6());
				})));
		eventloop.run();
	}

	@Test
	public void testCollectWithListener() {
		CollectListener<Object, Object[], List<Object>> any = (canceller, accumulator) -> {};
		eventloop.post(() -> Stages.collect(IndexedCollector.toList(), any, asList(Stage.of(1), Stage.of(2), Stage.of(3)))
				.whenComplete(assertComplete(list -> assertEquals(3, list.size()))));

		eventloop.run();
	}

	@Test
	public void testCollectStream() {
		eventloop.post(() -> Stages.collect(toList(), Stream.of(Stage.of(1), Stage.of(2), Stage.of(3)))
				.whenComplete(assertComplete(list -> assertEquals(3, list.size()))));

		eventloop.run();
	}
}
