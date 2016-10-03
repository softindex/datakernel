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
import com.google.inject.Stage;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Args;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import javax.inject.Singleton;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;

public class HttpHelloWorldLauncher extends Launcher {
	public static final int PORT = 11111;

	@Inject
	AsyncHttpServer httpServer;

	public HttpHelloWorldLauncher() {
		super(Stage.DEVELOPMENT,
				ServiceGraphModule.defaultInstance(),
				new AbstractModule() {
					@Override
					protected void configure() {
					}

					@Provides
					@Singleton
					Eventloop eventloop(@Args String[] args) {
						return Eventloop.create();
					}

					@Provides
					@Singleton
					AsyncHttpServer httpServer(Eventloop eventloop, AsyncServlet servlet) {
						return AsyncHttpServer.create(eventloop, servlet)
								.withListenPort(PORT);
					}

					@Provides
					@Singleton
					AsyncServlet httpServlet() {
						return new AsyncServlet() {
							@Override
							public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
								callback.setResult(HttpResponse.ok200().withBody(encodeAscii("Hello, World!")));
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
		main(HttpHelloWorldLauncher.class, args);
	}
}
