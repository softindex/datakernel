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

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.datastream.TestUtils.assertEndOfStream;
import static io.datakernel.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class ConsumerToListTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void emptyListTest() {
		StreamConsumerToList<String> consumer = StreamConsumerToList.create();

		List<String> testList2 = new ArrayList<>();
		testList2.add("a");
		testList2.add("b");
		testList2.add("c");
		testList2.add("d");

		StreamSupplier<String> supplier = StreamSupplier.ofIterable(testList2);
		await(supplier.streamTo(consumer));

		assertEquals(testList2, consumer.getList());
		assertEndOfStream(supplier);
	}

}
