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

package io.datakernel.examples;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.util.Collection;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;

public class HttpServerScratch extends Launcher {
	private final static int PORT = 25565;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(PORT))),
				new AbstractModule() {
					@Singleton
					@Provides
					Eventloop eventloop() {
						return Eventloop.create()
								.withFatalErrorHandler(rethrowOnAnyError());
					}

					@Singleton
					@Provides
					AsyncServlet servlet() {
						return request -> {
							logger.info("Received connection");
							return Promise.of(HttpResponse.ok200().withBody(encodeAscii("Hello from HTTP server")));
						};
					}

					@Singleton
					@Provides
					AsyncHttpServer server(Eventloop eventloop, AsyncServlet servlet, Config config) {
						return AsyncHttpServer.create(eventloop, servlet)
								.withListenAddress(config.get(ofInetSocketAddress(), Config.THIS));
					}
				}
		);
	}

	@Override
	protected void run() throws Exception {
		System.out.println("Server is running");
		System.out.println("You can connect from browser by visiting 'http://localhost:" + PORT + "'");
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpServerScratch();
		launcher.launch(true, args);
	}
}
