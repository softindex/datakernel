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

package io.datakernel.service;

import io.datakernel.boot.ServiceGraph;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static io.datakernel.service.TestServiceGraphServices.immediateFailedService;
import static io.datakernel.service.TestServiceGraphServices.immediateService;

public class ServiceGraphTest {

	@Test
	public void testStart() throws Exception {
		ServiceGraph graph = ServiceGraph.create();
		graph.add("x", immediateService(), "a", "b", "c");
		graph.add("y", immediateService(), "c");
		graph.add("z", immediateService(), "y", "x");
		System.out.println(graph);

		try {
			graph.startFuture().get();
		} finally {
			graph.stopFuture().get();
		}
	}

	@Test(expected = IllegalStateException.class)
	public void testWithCircularDependencies() throws Exception {
		ServiceGraph graph = ServiceGraph.create();
		graph.add("x", immediateService(), "a", "b", "c");
		graph.add("y", immediateService(), "c");
		graph.add("z", immediateService(), "y", "x");
		graph.add("t1", "t2");
		graph.add("t2", immediateService(), "t1");
		System.out.println(graph);

		try {
			graph.startFuture().get();
		} finally {
			graph.stopFuture().get();
		}
	}

	@Test(expected = ExecutionException.class)
	public void testBadNode() throws Exception {
		ServiceGraph graph = ServiceGraph.create();
		graph.add("a", immediateFailedService(new Exception()));
		graph.add("x", immediateService(), "a", "b", "c");
		graph.add("y", immediateService(), "c");
		graph.add("z", immediateService(), "y", "x");
		System.out.println(graph);

		try {
			graph.startFuture().get();
		} finally {
			graph.stopFuture().get();
		}
	}
}