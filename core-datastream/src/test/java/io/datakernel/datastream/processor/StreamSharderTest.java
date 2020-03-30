/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.datastream.processor;

import io.datakernel.common.exception.ExpectedException;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.datakernel.datastream.TestStreamTransformers.*;
import static io.datakernel.datastream.TestUtils.*;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamSharderTest {
	private static final Sharder<Integer> SHARDER = object -> object % 2;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void test1() {
		StreamSplitter<Integer, Integer> streamSharder = StreamSplitter.create(
				(item, acceptors) -> acceptors[SHARDER.shard(item)].accept(item));

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4);
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();

		await(
				source.streamTo(streamSharder.getInput()),
				streamSharder.newOutput().streamTo(consumer1.transformWith(randomlySuspending())),
				streamSharder.newOutput().streamTo(consumer2.transformWith(randomlySuspending()))
		);

		assertEquals(asList(2, 4), consumer1.getList());
		assertEquals(asList(1, 3), consumer2.getList());

		assertEndOfStream(source);
		assertEndOfStream(streamSharder.getInput());
		assertEndOfStream(streamSharder.getOutput(0));
		assertEndOfStream(consumer1);
		assertEndOfStream(consumer2);
	}

	@Test
	public void test2() {
		StreamSplitter<Integer, Integer> streamSharder = StreamSplitter.create(
				(item, acceptors) -> acceptors[SHARDER.shard(item)].accept(item));

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4);
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();

		await(
				source.streamTo(streamSharder.getInput()),
				streamSharder.newOutput().streamTo(consumer1.transformWith(randomlySuspending())),
				streamSharder.newOutput().streamTo(consumer2.transformWith(randomlySuspending()))
		);

		assertEquals(asList(2, 4), consumer1.getList());
		assertEquals(asList(1, 3), consumer2.getList());

		assertEndOfStream(source);
		assertEndOfStream(source);
		assertEndOfStream(streamSharder.getInput());
		assertEndOfStream(streamSharder.getOutput(0));
		assertEndOfStream(consumer1);
		assertEndOfStream(consumer2);
	}

	@Test
	public void testWithError() {
		StreamSplitter<Integer, Integer> streamSharder = StreamSplitter.create(
				(item, acceptors) -> acceptors[SHARDER.shard(item)].accept(item));

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4);

		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();
		ExpectedException exception = new ExpectedException("Test Exception");

		Throwable e = awaitException(
				source.streamTo(streamSharder.getInput()),
				streamSharder.newOutput().streamTo(consumer1),
				streamSharder.newOutput().streamTo(
						consumer2.transformWith(decorate(promise ->
								promise.then(item -> item == 3 ? Promise.ofException(exception) : Promise.of(item))))));

		assertSame(exception, e);
		assertEquals(1, consumer1.getList().size());
		assertEquals(2, consumer2.getList().size());
		assertClosedWithError(source);
		assertClosedWithError(source);
		assertClosedWithError(streamSharder.getInput());
		assertSuppliersClosedWithError(streamSharder.getOutputs());
		assertClosedWithError(consumer1);
		assertClosedWithError(consumer2);
	}

	@Test
	public void testSupplierWithError() {
		StreamSplitter<Integer, Integer> streamSharder = StreamSplitter.create(
				(item, acceptors) -> acceptors[SHARDER.shard(item)].accept(item));

		ExpectedException exception = new ExpectedException("Test Exception");

		StreamSupplier<Integer> source = StreamSupplier.concat(
				StreamSupplier.of(1),
				StreamSupplier.of(2),
				StreamSupplier.of(3),
				StreamSupplier.closingWithError(exception)
		);

		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();

		Throwable e = awaitException(
				source.streamTo(streamSharder.getInput()),
				streamSharder.newOutput().streamTo(consumer1.transformWith(oneByOne())),
				streamSharder.newOutput().streamTo(consumer2.transformWith(oneByOne()))
		);

		assertSame(exception, e);
		assertEquals(1, consumer1.getList().size());
		assertEquals(2, consumer2.getList().size());

		assertClosedWithError(streamSharder.getInput());
		assertSuppliersClosedWithError(streamSharder.getOutputs());
		assertClosedWithError(consumer1);
		assertClosedWithError(consumer2);
	}
}
