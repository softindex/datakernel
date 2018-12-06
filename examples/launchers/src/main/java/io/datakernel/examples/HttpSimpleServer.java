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
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.Collection;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.launchers.http.HttpServerLauncher.EAGER_SINGLETONS_MODE;
import static java.lang.Boolean.parseBoolean;
import static java.util.Collections.singletonList;

public class HttpSimpleServer {
	private static final int SERVICE_PORT = 25565;

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(
						new AbstractModule() {
							@Singleton
							@Provides
							AsyncServlet rootServlet() {
								return request -> {
									logger.info("Connection received");
									return Promise.of(HttpResponse.ok200().withBody(encodeAscii("Hello from HTTP server")));
								};
							}
						}
				);
			}

			@Override
			protected Collection<Module> getOverrideModules() {
				return singletonList(
						ConfigModule.create(Config.create()
								.with("http.listenAddresses", "" + SERVICE_PORT)
						)
				);
			}

			@Override
			protected void run() throws Exception {
				System.out.println("Server is running");
				System.out.println("You can connect from browser by visiting 'http://localhost:" + SERVICE_PORT + "'");
				awaitShutdown();
			}
		};

		launcher.launch(parseBoolean(EAGER_SINGLETONS_MODE), args);
	}
}
