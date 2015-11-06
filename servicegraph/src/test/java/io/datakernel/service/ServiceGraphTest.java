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

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioServer;
import io.datakernel.eventloop.NioService;
import io.datakernel.eventloop.PrimaryNioServer;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

public class ServiceGraphTest {

	private static ServiceGraph.Node stringNode(String s) {
		return new ServiceGraph.Node(s, ConcurrentServices.immediateService());
	}

	@Test
	public void testStart() throws Exception {
		ServiceGraph graph = new ServiceGraph();
		graph.add(stringNode("x"), stringNode("a"), stringNode("b"), stringNode("c"));
		graph.add(stringNode("y"), stringNode("c"));
		graph.add(stringNode("z"), stringNode("y"), stringNode("x"));
		graph.add(new ServiceGraph.Node("t1", null), new ServiceGraph.Node("t2", ConcurrentServices.immediateService()));
		graph.add(new ServiceGraph.Node("t2", ConcurrentServices.immediateService()), new ServiceGraph.Node("t1", null));

		try {
			ConcurrentServiceCallbacks.CountDownServiceCallback callback = ConcurrentServiceCallbacks.withCountDownLatch();
			graph.startFuture(callback);
			callback.await();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			ConcurrentServiceCallbacks.CountDownServiceCallback callback = ConcurrentServiceCallbacks.withCountDownLatch();
			graph.stopFuture(callback);
			callback.await();
		}
	}

	@Test
	public void test2() throws InterruptedException {
		final NioEventloop eventloop = new NioEventloop();
		ServiceGraph graph = new ServiceGraph();



		graph.add(ServiceGraph.Node.ofNioService("nioService1" , new NioService() {
			private NioEventloop eventloop = new NioEventloop();

			@Override
			public NioEventloop getNioEventloop() {
				return eventloop;
			}

			@Override
			public void start(CompletionCallback callback) {
				callback.onComplete();
			}

			@Override
			public void stop(CompletionCallback callback) {
				callback.onComplete();
			}
		}), stringNode("a"), stringNode("b"), stringNode("c"));

		try {
			ConcurrentServiceCallbacks.CountDownServiceCallback callback = ConcurrentServiceCallbacks.withCountDownLatch();
			graph.startFuture(callback);
			callback.await();
			System.out.println("\n\n\n");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			ConcurrentServiceCallbacks.CountDownServiceCallback callback = ConcurrentServiceCallbacks.withCountDownLatch();
			graph.stopFuture(callback);
			callback.await();
		}
	}

	@Test
	public void testNioService() throws InterruptedException {
		final ArrayList<NioEventloopRunner> workerServices = new ArrayList<>();
		final ArrayList<NioServer> workerServers = new ArrayList<>();
		ServiceGraph.Node[] nodes = new ServiceGraph.Node[4];

		for (int i = 0; i < 4; i++) {
			final NioEventloop workerEventloop = new NioEventloop();
			NioServer workerServer = new NioServer() {
				@Override
				public NioEventloop getNioEventloop() {
					return workerEventloop;
				}

				@Override
				public void listen() throws IOException {

				}

				@Override
				public void close() {

				}

				@Override
				public void onAccept(SocketChannel socketChannel) {

				}
			};
			workerServers.add(workerServer);

			workerServices.add(new NioEventloopRunner(workerEventloop)
					.addNioServers(workerServer));
			nodes[i] = ServiceGraph.Node.ofNioServer("workerEventloop" + String.valueOf(i), workerServer);
		}

		int PORT = 9444;
		NioEventloop primaryEventloop = new NioEventloop();
		PrimaryNioServer primaryNioServer = PrimaryNioServer.create(primaryEventloop)
				.workerNioServers(workerServers)
				.setListenPort(PORT);

		ServiceGraph graph = new ServiceGraph();
//		graph.add(new ServiceGraph.Node("primaryNioServer", ConcurrentServices.concurrentServiceOfNioServer(primaryNioServer)), nodes);
		graph.add(ServiceGraph.Node.ofNioServer("primaryNioServer", primaryNioServer), nodes);

		try {
			ConcurrentServiceCallbacks.CountDownServiceCallback callback = ConcurrentServiceCallbacks.withCountDownLatch();
			graph.startFuture(callback);
			callback.await();
			System.out.println("\n\n\n");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			ConcurrentServiceCallbacks.CountDownServiceCallback callback = ConcurrentServiceCallbacks.withCountDownLatch();
			graph.stopFuture(callback);
			callback.await();
		}
	}
}