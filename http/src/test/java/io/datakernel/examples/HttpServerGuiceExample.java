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
import io.datakernel.eventloop.NioServer;
import io.datakernel.eventloop.PrimaryNioServer;
import io.datakernel.guice.servicegraph.AsyncServiceAdapters;
import io.datakernel.guice.servicegraph.ServiceGraphModule;
import io.datakernel.guice.servicegraph.SingletonService;
import io.datakernel.guice.workers.NioWorkerModule;
import io.datakernel.guice.workers.NioWorkerScopeFactory;
import io.datakernel.guice.workers.WorkerId;
import io.datakernel.guice.workers.WorkerThread;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.service.AsyncServiceCallbacks;
import io.datakernel.service.ServiceGraph;

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
			install(new ServiceGraphModule()
							.register(NioServer.class, AsyncServiceAdapters.forNioServer())
							.register(NioEventloop.class, AsyncServiceAdapters.forNioEventloop())
			);
		}

		@Provides
		@SingletonService
		NioEventloop primaryEventloop() {
			return new NioEventloop();
		}

		@Provides
		@SingletonService
		PrimaryNioServer primaryNioServer(NioEventloop primaryEventloop,
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
		AsyncHttpServer workerHttpServer(@WorkerThread NioEventloop eventloop, @WorkerId final int workerId) {
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
		List<AsyncHttpServer> workerHttpServers(NioWorkerScopeFactory nioWorkerScope, @WorkerThread Provider<AsyncHttpServer> itemProvider) {
			return nioWorkerScope.getList(WORKERS, itemProvider);
		}
	}

	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new TestModule());
		ServiceGraph graph = ServiceGraphModule.getServiceGraph(injector, PrimaryNioServer.class);
		try {
			AsyncServiceCallbacks.BlockingServiceCallback callback = AsyncServiceCallbacks.withCountDownLatch();
			graph.start(callback);
			callback.await();

			System.out.format("Server started at http://localhost:%d/, press 'enter' to shut it down.", PORT);
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			br.readLine();
		} finally {
			AsyncServiceCallbacks.BlockingServiceCallback callback = AsyncServiceCallbacks.withCountDownLatch();
			graph.stop(callback);
			callback.await();
		}
	}
}
