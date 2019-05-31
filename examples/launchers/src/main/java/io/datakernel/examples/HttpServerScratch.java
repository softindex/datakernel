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

import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.Inject;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.di.module.Modules.combine;

public class HttpServerScratch extends Launcher {
	private static final int PORT = 8080;
	@Inject
	private AsyncHttpServer server;

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(PORT))),
				new AbstractModule() {
					@Provides
					Eventloop eventloop() {
						return Eventloop.create();
					}

					@Provides
					AsyncServlet servlet() {
						return request -> Promise.of(HttpResponse.ok200()
								.withBody(encodeAscii("Hello from HTTP server")));
					}

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
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpServerScratch();
		launcher.launch(args);
	}
}
