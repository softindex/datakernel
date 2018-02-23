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

package io.datakernel.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.trigger.Severity;
import io.datakernel.trigger.TriggerResult;
import io.datakernel.trigger.TriggersModule;

import javax.inject.Singleton;
import java.net.InetSocketAddress;

import static com.google.inject.Stage.PRODUCTION;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;

public class HttpHelloWorldLauncher extends Launcher {
	private static class HttpHelloWorldModule extends AbstractModule {
		@Override
		protected void configure() {
		}

		@Provides
		@Singleton
		Eventloop eventloop() {
			return Eventloop.create();
		}

		@Provides
		@Singleton
		AsyncHttpServer httpServer(Eventloop eventloop, AsyncServlet rootServlet) {
			return AsyncHttpServer.create(eventloop, rootServlet)
					.withListenAddress(new InetSocketAddress("localhost", PORT));
		}

		@Provides
		@Singleton
		AsyncServlet rootServlet() {
			return new AsyncServlet() {
				@Override
				public Stage<HttpResponse> serve(HttpRequest request) {
					return Stage.of(HttpResponse.ok200().withBody(encodeAscii("Hello, World!")));
				}
			};
		}
	}

	public static final int PORT = 11111;

	@Inject
	AsyncHttpServer httpServer;

	public HttpHelloWorldLauncher() {
		super(PRODUCTION,
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				TriggersModule.create()
						.with(Eventloop.class, Severity.DEBUG, "Debug", eventloop ->
								TriggerResult.create()
										.when(eventloop.getLoop() % 3 != 0))
						.with(Eventloop.class, Severity.WARNING, "Debug", eventloop ->
								TriggerResult.create()
										.when(eventloop.getLoop() % 4 != 0))
						.with(Eventloop.class, Severity.HIGH, "FatalErrors", eventloop ->
								TriggerResult.ofError(eventloop.getStats().getFatalErrors()))
						.with(AsyncHttpServer.class, Severity.AVERAGE, "HttpErrors", server ->
								TriggerResult.ofError(server.getStats().getHttpErrors())),
				new HttpHelloWorldModule());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		main(HttpHelloWorldLauncher.class, args);
	}

}
