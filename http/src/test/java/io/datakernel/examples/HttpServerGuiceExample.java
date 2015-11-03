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

package io.datakernel.examples;

import com.google.inject.*;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.PrimaryNioServer;
import io.datakernel.guice.workers.*;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.service.ConcurrentServiceCallbacks;
import io.datakernel.service.NioEventloopRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import static io.datakernel.util.ByteBufStrings.encodeAscii;

/**
 * Example of instantiating PrimaryNioServer with worker servers using Guice dependency injection framework.
 */
public class HttpServerGuiceExample {
	public static final int PORT = 7583;
	public static final int WORKERS = 4;

	/* Guice module which is responsible for creating various 'components' of a primary server with workers. */
	public static class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			install(new NioWorkerModule());
		}

		@Provides
		@Singleton
		@PrimaryThread
		NioEventloop primaryEventloop() {
			return new NioEventloop();
		}

		@Provides
		@Singleton
		@PrimaryThread
		NioEventloopRunner primaryEventloopRunner(@PrimaryThread NioEventloop primaryEventloop,
		                                          PrimaryNioServer primaryNioServer,
		                                          List<NioEventloopRunner> workerNioEventloopRunners) {
			NioEventloopRunner eventloopRunner = new NioEventloopRunner(primaryEventloop);
			eventloopRunner.addNioServers(primaryNioServer);
			eventloopRunner.addConcurrentServices(workerNioEventloopRunners);
			return eventloopRunner;
		}

		@Provides
		@Singleton
		PrimaryNioServer primaryNioServer(@PrimaryThread NioEventloop primaryEventloop,
		                                  List<AsyncHttpServer> workerHttpServers) {
			PrimaryNioServer primaryNioServer = PrimaryNioServer.create(primaryEventloop);
			primaryNioServer.workerNioServers(workerHttpServers);
			primaryNioServer.setListenPort(PORT);
			return primaryNioServer;
		}

		@Provides
		@WorkerThread
		NioEventloop workerEventloop() {
			return new NioEventloop();
		}

		@Provides
		@WorkerThread
		NioEventloopRunner workerEventloopRunner(NioEventloop eventloop, AsyncHttpServer httpServer) {
			NioEventloopRunner eventloopRunner = new NioEventloopRunner(eventloop);
			eventloopRunner.addNioServers(httpServer);
			return eventloopRunner;
		}

		@Provides
		@Singleton
		List<NioEventloopRunner> workerEventloopRunners(NioWorkerScope nioWorkerScope,
		                                                Provider<NioEventloopRunner> itemProvider) {
			return nioWorkerScope.getList(WORKERS, itemProvider);
		}

		@Provides
		@WorkerThread
		AsyncHttpServer workerHttpServer(NioEventloop eventloop, @WorkerId final int workerId) {
			return new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
				@Override
				public void serveAsync(HttpRequest request,
				                       ResultCallback<HttpResponse> callback) {
					HttpResponse httpResponse = HttpResponse.create(200);
					httpResponse.body(ByteBuf.wrap(encodeAscii("Hello world: worker server #" + workerId)));
					callback.onResult(httpResponse);
				}
			});
		}

		@Provides
		@Singleton
		List<AsyncHttpServer> workerHttpServers(NioWorkerScope nioWorkerScope, Provider<AsyncHttpServer> itemProvider) {
			return nioWorkerScope.getList(WORKERS, itemProvider);
		}
	}

	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new TestModule());
		NioEventloopRunner primaryNioEventloopRunner = injector.getInstance(Key.get(NioEventloopRunner.class,
				PrimaryThread.class));
		try {
			ConcurrentServiceCallbacks.CountDownServiceCallback callback = ConcurrentServiceCallbacks.withCountDownLatch();
			primaryNioEventloopRunner.startFuture(callback);
			callback.await();

			System.out.format("Server started at http://localhost:%d/, press 'enter' to shut it down.", PORT);
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			br.readLine();
		} finally {
			ConcurrentServiceCallbacks.CountDownServiceCallback callback = ConcurrentServiceCallbacks.withCountDownLatch();
			primaryNioEventloopRunner.stopFuture(callback);
			callback.await();
		}
	}
}
