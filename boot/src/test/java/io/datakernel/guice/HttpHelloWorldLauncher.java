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

import com.google.inject.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AbstractAsyncServlet;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import javax.inject.Singleton;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;

public class HttpHelloWorldLauncher extends Launcher {
	public static final int PORT = 11111;

	@Override
	public Injector getInjector() {
		return Guice.createInjector(Stage.PRODUCTION,
				ServiceGraphModule.defaultInstance(),
				new AbstractModule() {
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
					AsyncHttpServer httpServer(Eventloop eventloop, AbstractAsyncServlet servlet) {
						return AsyncHttpServer.create(eventloop, servlet)
								.withListenPort(PORT);
					}

					@Provides
					@Singleton
					AbstractAsyncServlet httpServlet(Eventloop eventloop) {
						return new AbstractAsyncServlet(eventloop) {
							@Override
							protected void doServeAsync(HttpRequest request, Callback callback) throws ParseException {
								callback.setResponse(HttpResponse.ok200().withBody(encodeAscii("Hello, World!")));
							}
						};
					}
				});
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		run(HttpHelloWorldLauncher.class, args);
	}
}
