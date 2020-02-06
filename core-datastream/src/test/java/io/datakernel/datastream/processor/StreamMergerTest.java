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

import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.function.Function;

import static io.datakernel.datastream.TestStreamConsumers.*;
import static io.datakernel.datastream.TestUtils.*;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamMergerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testDeduplicate() {
		StreamSupplier<Integer> source0 = StreamSupplier.ofIterable(Collections.emptyList());
		StreamSupplier<Integer> source1 = StreamSupplier.of(3, 7);
		StreamSupplier<Integer> source2 = StreamSupplier.of(3, 4, 6);

		StreamMerger<Integer, Integer> merger = StreamMerger.create(Function.identity(), Integer::compareTo, true);

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(
				source0.streamTo(merger.newInput()),
				source1.streamTo(merger.newInput()),
				source2.streamTo(merger.newInput()),

				merger.getOutput()
						.streamTo(consumer.transformWith(randomlySuspending()))
		);

		assertEquals(asList(3, 4, 6, 7), consumer.getList());

		assertEndOfStream(source0);
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(consumer);
		assertEndOfStream(merger.getOutput());
		assertConsumersEndOfStream(merger.getInputs());
	}

	@Test
	public void testDuplicate() {
		StreamSupplier<Integer> source0 = StreamSupplier.ofIterable(Collections.emptyList());
		StreamSupplier<Integer> source1 = StreamSupplier.of(3, 7);
		StreamSupplier<Integer> source2 = StreamSupplier.of(3, 4, 6);

		StreamMerger<Integer, Integer> merger = StreamMerger.create(Function.identity(), Integer::compareTo, false);

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(
				source0.streamTo(merger.newInput()),
				source1.streamTo(merger.newInput()),
				source2.streamTo(merger.newInput()),
				merger.getOutput()
						.streamTo(consumer.transformWith(randomlySuspending()))
		);

		assertEquals(asList(3, 3, 4, 6, 7), consumer.getList());

		assertEndOfStream(source0);
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(consumer);
		assertEndOfStream(merger.getOutput());
		assertConsumersEndOfStream(merger.getInputs());
	}

	@Test
	public void test() {
		DataItem1 d0 = new DataItem1(0, 1, 1, 1);
		DataItem1 d1 = new DataItem1(0, 2, 1, 2);
		DataItem1 d2 = new DataItem1(0, 6, 1, 3);
		DataItem1 d3 = new DataItem1(1, 1, 1, 4);
		DataItem1 d4 = new DataItem1(1, 5, 1, 5);

		StreamSupplier<DataItem1> source1 = StreamSupplier.ofIterable(
				asList(d0, //DataItem1(0,1,1,1)
						d1, //DataItem1(0,2,1,2)
						d2  //DataItem1(0,6,1,3)
				));
		StreamSupplier<DataItem1> source2 = StreamSupplier.ofIterable(
				asList(d3,//DataItem1(1,1,1,4)
						d4 //DataItem1(1,5,1,5)
				));

		StreamMerger<Integer, DataItem1> merger = StreamMerger.create(
				input -> input.key2, Integer::compareTo, false);

		StreamConsumerToList<DataItem1> consumer = StreamConsumerToList.create();

		await(
				source1.streamTo(merger.newInput()),
				source2.streamTo(merger.newInput()),
				merger.getOutput()
						.streamTo(consumer.transformWith(oneByOne()))
		);

		assertEquals(asList(d0, //DataItem1(0,1,1,1)
				d3, //DataItem1(1,1,1,4)
				d1, //DataItem1(0,2,1,2)
				d4, //DataItem1(1,5,1,5)
				d2  //DataItem1(0,6,1,3)
		), consumer.getList());

		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(consumer);
		assertEndOfStream(merger.getOutput());
		assertConsumersEndOfStream(merger.getInputs());
	}

	@Test
	public void testDeduplicateWithError() {
		StreamSupplier<Integer> source1 = StreamSupplier.of(7, 8);
		StreamSupplier<Integer> source2 = StreamSupplier.of(3, 4, 6);

		StreamMerger<Integer, Integer> merger = StreamMerger.create(Function.identity(), Integer::compareTo, true);

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Exception exception = new Exception("Test Exception");

		Throwable e = awaitException(
				source1.streamTo(merger.newInput()),
				source2.streamTo(merger.newInput()),
				merger.getOutput()
						.streamTo(consumer
								.transformWith(decorate(promise -> promise.then(
										item -> item == 8 ? Promise.ofException(exception) : Promise.of(item)))))
		);

		assertSame(exception, e);
		assertEquals(5, consumer.getList().size());
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertClosedWithError(consumer);
		assertClosedWithError(merger.getOutput());
		assertEndOfStream(merger.getInput(0));
		assertEndOfStream(merger.getInput(1));
	}

	@Test
	public void testSupplierDeduplicateWithError() {
		StreamSupplier<Integer> source1 = StreamSupplier.concat(
				StreamSupplier.of(7),
				StreamSupplier.of(8),
				StreamSupplier.closingWithError(new Exception("Test Exception")),
				StreamSupplier.of(3),
				StreamSupplier.of(9)
		);
		StreamSupplier<Integer> source2 = StreamSupplier.concat(
				StreamSupplier.of(3),
				StreamSupplier.of(4),
				StreamSupplier.of(6),
				StreamSupplier.of(9)
		);

		StreamMerger<Integer, Integer> merger = StreamMerger.create(Function.identity(), Integer::compareTo, true);

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		awaitException(
				source1.streamTo(merger.newInput()),
				source2.streamTo(merger.newInput()),
				merger.getOutput()
						.streamTo(consumer.transformWith(oneByOne()))
		);

		assertEquals(5, consumer.getList().size());
		assertClosedWithError(consumer);
		assertClosedWithError(merger.getOutput());
		assertClosedWithError(merger.getInput(0));
		assertEndOfStream(merger.getInput(1));
	}

}
