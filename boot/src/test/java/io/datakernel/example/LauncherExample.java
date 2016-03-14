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
import com.google.inject.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.config.PropertiesConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncHttpServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;
import io.datakernel.worker.WorkerPool;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.config.ConfigConverters.ofString;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

public class LauncherExample extends Launcher {

	public static void main(String[] args) throws Exception {
		Launcher.run(LauncherExample.class, args);
	}

	@Override
	protected void configure() {
		injector(Stage.PRODUCTION,
				ServiceGraphModule.defaultInstance(),
				new JmxModule(),
				new PropertiesConfigModule("configs.properties"),
				new LauncherExampleModule());
	}

	@Override
	protected void doRun() throws Exception {
		awaitShutdown();
	}

	public static class LauncherExampleModule extends AbstractModule {
		@Override
		protected void configure() {
		}

		@Provides
		@Singleton
		WorkerPool workerPool(Config config) {
			return new WorkerPool(config.get(ofInteger(), "workers", 7));
		}

		@Provides
		@Singleton
		Eventloop primaryEventloop() {
			return new Eventloop();
		}

		@Provides
		@Singleton
		PrimaryServer primaryServer(Eventloop primaryEventloop, WorkerPool workerPool, Config config) {
			PrimaryServer primaryNioServer = PrimaryServer.create(primaryEventloop);
			primaryNioServer.workerServers(workerPool.getInstances(AsyncHttpServer.class));
			int port = config.get(ofInteger(), "port", 5577);
			primaryNioServer.setListenPort(port);
			return primaryNioServer;
		}

		@Provides
		@Worker
		Eventloop workerEventloop() {
			return new Eventloop();
		}

		@Provides
		@Worker
		AsyncHttpServer workerHttpServer(@Worker Eventloop eventloop, @WorkerId final int workerId, Config config) {
			final String responseMessage = config.get(ofString(), "msg", "Some msg");
			return new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
				@Override
				public void serveAsync(HttpRequest request,
				                       Callback callback) {
					HttpResponse httpResponse = HttpResponse.create(200);
					httpResponse.body(ByteBuf.wrap(encodeAscii(
							"Worker server #" + workerId + ". Message: " + responseMessage + "\n")));
					callback.onResult(httpResponse);
				}
			});
		}
	}
}
