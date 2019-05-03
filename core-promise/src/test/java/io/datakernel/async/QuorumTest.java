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

import io.datakernel.exception.StacklessException;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.util.ref.RefInt;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

public final class QuorumTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void simple() {
		Iterator<Function<Integer, Promise<String>>> functions = Stream.<Function<Integer, Promise<String>>>of(
				i -> Promise.of("" + i),
				i -> Promise.of("" + (i + 1)),
				i -> Promise.of("" + (i + 2)),
				i -> Promise.ofException(new StacklessException(QuorumTest.class, "test")),
				i -> Promise.of("" + (i + 3))
		).iterator();

		Function<Integer, Promise<String>> quorum = Quorum.create(functions, list -> String.join("", list), 4, 2);
		String result = await(quorum.apply(1));
		assertEquals(Stream.of(49, 50, 51, 52).collect(toSet()), result.chars().boxed().collect(toSet()));
	}

	@Test
	public void error() {
		Iterator<Function<Integer, Promise<String>>> functions = Stream.<Function<Integer, Promise<String>>>of(
				i -> Promise.of("" + i),
				i -> Promise.ofException(new StacklessException(QuorumTest.class, "test")),
				i -> Promise.of("" + (i + 2)),
				i -> Promise.ofException(new StacklessException(QuorumTest.class, "test2")),
				i -> Promise.of("" + (i + 3)),
				i -> Promise.ofException(new StacklessException(QuorumTest.class, "test3"))
		).iterator();

		Function<Integer, Promise<String>> quorum = Quorum.create(functions, list -> String.join("", list), 4, 2);
		Throwable e = awaitException(quorum.apply(1));

		assertThat(e.getMessage(), containsString("Not enough successful completions, 4 were required, only 3 succeeded"));
	}

	private static Promise<String> work(long millis, RefInt calls, int maxCalls, String res) {
		return Promise.ofCallback(cb -> {
			assertTrue("More parrallel calls than the limit!", calls.inc() <= maxCalls);
			getCurrentEventloop().delay(millis, () -> {
				calls.dec();
				cb.set(res);
			});
		});
	}

	@Test
	public void parallel() {
		int maxParallelCalls = 4;
		RefInt calls = new RefInt(0);
		Iterator<Function<Integer, Promise<String>>> functions = Stream.<Function<Integer, Promise<String>>>of(
				i -> work(50, calls, maxParallelCalls, "" + i),
				i -> work(40, calls, maxParallelCalls, "" + (i + 1)),
				i -> work(60, calls, maxParallelCalls, "" + (i + 2)),
				i -> work(40, calls, maxParallelCalls, "" + (i + 3)),
				i -> work(60, calls, maxParallelCalls, "" + (i + 4)),
				i -> work(50, calls, maxParallelCalls, "" + (i + 5)),
				i -> work(40, calls, maxParallelCalls, "" + (i + 6)),
				i -> work(50, calls, maxParallelCalls, "" + (i + 7)),
				i -> work(35, calls, maxParallelCalls, "" + (i + 8)),
				i -> work(70, calls, maxParallelCalls, "" + (i + 9))
		).iterator();

		Function<Integer, Promise<String>> quorum = Quorum.create(functions, list -> String.join("", list), 8, maxParallelCalls);
		await(quorum.apply(1));
	}
}
