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
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncHttpServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import static io.datakernel.util.ByteBufStrings.encodeAscii;

public class SimpleHttpLauncherExample extends Launcher {
	public static void main(String[] args) throws Exception {
		Launcher.run(SimpleHttpLauncherExample.class, args);
	}

	@Override
	protected void configure() {
		configs("launcher-example.properties");
		injector(Stage.PRODUCTION,
				ServiceGraphModule.defaultInstance(),
				new LauncherExampleModule());
	}

	@Override
	protected void doRun() throws Exception {
		awaitShutdown();
	}

	public static class LauncherExampleModule extends AbstractModule {
		private static final String DEFAULT_RESPONSE_MESSAGE = "Hello, World!";
		public static final int DEFAULT_PORT = 5561;

		@Override
		protected void configure() {
		}

		@Provides
		@Singleton
		Eventloop workerEventloop() {
			return new Eventloop();
		}

		@Provides
		@Singleton
		AsyncHttpServer workerHttpServer(Eventloop eventloop, final Config config) {
			AsyncHttpServer httpServer = new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
				@Override
				public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
					String responseMessage = config.get(ConfigConverters.ofString(), "responseMessage", DEFAULT_RESPONSE_MESSAGE);
					HttpResponse content = HttpResponse.create().body(ByteBuf.wrap(encodeAscii(
							"Message: " + responseMessage + "\n")));
					callback.onResult(content);
				}
			});
			int port = config.get(ConfigConverters.ofInteger(), "port", DEFAULT_PORT);
			return httpServer.setListenPort(port);
		}

	}
}
