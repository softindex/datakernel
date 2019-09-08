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

package io.datakernel.worker;

import io.datakernel.common.ref.RefInt;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static io.datakernel.common.collection.CollectionUtils.set;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class WorkerPoolTest {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private WorkerPool first;
	private WorkerPool second;
	private WorkerPools pools;

	@Before
	public void setUp() {
		RefInt counter = new RefInt(0);
		Injector injector = Injector.of(
				new AbstractModule() {
					@Override
					protected void configure() {
						bind(String.class).in(Worker.class).to(() -> "String: " + counter.value++);
					}
				},
				WorkerPoolModule.create());

		pools = injector.getInstance(WorkerPools.class);
		first = pools.createPool(4);
		second = pools.createPool(10);
	}

	@Test
	public void addedWorkerPools() {
		assertEquals(set(first, second), new HashSet<>(pools.getWorkerPools()));
	}

	@Test
	public void numberOfInstances() {
		assertEquals(4, first.getInstances(String.class).size());
		assertEquals(10, second.getInstances(String.class).size());
	}

	@Test
	public void numberOfCalls() {
		Set<String> actual = new HashSet<>();
		actual.addAll(first.getInstances(String.class).getList());
		actual.addAll(second.getInstances(String.class).getList());
		Set<String> expected = IntStream.range(0, 14).mapToObj(i -> "String: " + i).collect(toSet());
		assertEquals(expected, actual);
	}
}
