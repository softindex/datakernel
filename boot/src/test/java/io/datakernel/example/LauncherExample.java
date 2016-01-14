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

package io.datakernel.example;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.ResultCallback;
import io.datakernel.boot.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.PrimaryNioServer;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.launcher.Launcher;

import static io.datakernel.util.ByteBufStrings.encodeAscii;

public class LauncherExample {

	public static void main(String[] args) throws Exception {
		Launcher.run(ServicesLauncher.class, args);
	}

	public static class ServicesLauncher extends Launcher {
		@Override
		protected void configure() {
			configs("launcher-example.properties");
			modules(BootModule.defaultInstance(),
					new LauncherExampleModule());
		}

		@Override
		protected void doRun() throws Exception {
			awaitShutdown();
		}
	}

	public static class LauncherExampleModule extends AbstractModule {
		@Override
		protected void configure() {
		}

		@Provides
		@Singleton
		WorkerPool workerPool(WorkerPoolFactory factory, Config config) {
			return factory.createPool(config.get(ConfigConverters.ofInteger(), "workers", 4));
		}

		@Provides
		@Singleton
		NioEventloop primaryEventloop() {
			return new NioEventloop();
		}

		@Provides
		@Singleton
		PrimaryNioServer primaryNioServer(NioEventloop primaryEventloop, WorkerPool workerPool,
		                                  Config config) {
			PrimaryNioServer primaryNioServer = PrimaryNioServer.create(primaryEventloop);
			primaryNioServer.workerNioServers(workerPool.getInstances(AsyncHttpServer.class));
			int port = config.get(ConfigConverters.ofInteger(), "port", 5577);
			primaryNioServer.setListenPort(port);
			return primaryNioServer;
		}

		@Provides
		@Worker
		NioEventloop workerEventloop() {
			return new NioEventloop();
		}

		@Provides
		@Worker
		AsyncHttpServer workerHttpServer(@Worker NioEventloop eventloop, @WorkerId final int workerId,
		                                 Config config) {
			final String responseMessage = config.get(ConfigConverters.ofString(), "responseMessage", "Hello, World!");
			return new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
				@Override
				public void serveAsync(HttpRequest request,
				                       ResultCallback<HttpResponse> callback) {
					HttpResponse httpResponse = HttpResponse.create(200);
					httpResponse.body(ByteBuf.wrap(encodeAscii(
							"Worker server #" + workerId + ". Message: " + responseMessage + "\n")));
					callback.onResult(httpResponse);
				}
			});
		}

	}
}
