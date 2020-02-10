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
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.datakernel.datastream.TestUtils.assertEndOfStream;
import static io.datakernel.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class StreamSupplierOfIteratorTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void test1() {
		List<Integer> list = Arrays.asList(1, 2, 3);

		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(list);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(supplier.streamTo(consumer));

		assertEquals(list, consumer.getList());
		assertEndOfStream(supplier);
		assertEndOfStream(consumer);
	}

}


