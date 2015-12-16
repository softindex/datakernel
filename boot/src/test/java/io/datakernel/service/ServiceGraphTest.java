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

import org.junit.Test;

public class ServiceGraphTest {

	private static ServiceGraph.Node stringNode(String s) {
		return new ServiceGraph.Node(s, AsyncServices.immediateService());
	}

	private static ServiceGraph.Node badStringNode(String s) {
		return new ServiceGraph.Node(s, AsyncServices.immediateFailedService(new RuntimeException("Can't process service: " + s)));
	}

	@Test
	public void testStart() throws Exception {
		ServiceGraph graph = new ServiceGraph();
		graph.add(stringNode("x"), stringNode("a"), stringNode("b"), stringNode("c"));
		graph.add(stringNode("y"), stringNode("c"));
		graph.add(stringNode("z"), stringNode("y"), stringNode("x"));

		try {
			graph.start();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			graph.stop();
		}
	}

	@Test
	public void testWithCircularDependencies() throws Exception {
		ServiceGraph graph = new ServiceGraph() {
			@Override
			protected void onStart() {
				breakCircularDependencies();
			}
		};
		graph.add(stringNode("x"), stringNode("a"), stringNode("b"), stringNode("c"));
		graph.add(stringNode("y"), stringNode("c"));
		graph.add(stringNode("z"), stringNode("y"), stringNode("x"));
		graph.add(new ServiceGraph.Node("t1", null), new ServiceGraph.Node("t2", AsyncServices.immediateService()));
		graph.add(new ServiceGraph.Node("t2", AsyncServices.immediateService()), new ServiceGraph.Node("t1", null));

		try {
			graph.start();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			graph.stop();
		}
	}

	@Test
	public void testBadNode() throws Exception {
		ServiceGraph graph = new ServiceGraph();
		graph.add(stringNode("x"), badStringNode("a"), stringNode("b"), stringNode("c"));
		graph.add(stringNode("y"), stringNode("c"));
		graph.add(stringNode("z"), stringNode("y"), stringNode("x"));

		try {
			graph.start();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			graph.stop();
		}
	}
}