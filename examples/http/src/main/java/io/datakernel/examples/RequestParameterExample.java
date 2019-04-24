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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.loader.StaticLoaders;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.util.CollectionUtils.list;
import static java.lang.Boolean.parseBoolean;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class RequestParameterExample extends HttpServerLauncher {
	private static final Path RESOURCE_DIR = Paths.get("src/main/resources/static/query");

	@Override
	protected Collection<Module> getBusinessLogicModules() {
		return list(new AbstractModule() {
			@Provides
			@Singleton
			AsyncServlet mainServlet(Eventloop eventloop) {
				return RoutingServlet.create()
						.with(HttpMethod.POST, "/hello", request -> request
								.getPostParameters()
								.map(postParameters -> {
									String name = postParameters.get("name");
									return HttpResponse.ok200()
											.withBody(wrapUtf8("<h1><center>Hello from POST, " + name + "!</center></h1>"));
								}))
						.with(HttpMethod.GET, "/hello", request -> {
							String name = request.getQueryParameterOrNull("name");
							return Promise.of(HttpResponse.ok200()
									.withBody(wrapUtf8("<h1><center>Hello from GET, " + name + "!</center></h1>")));
						})
						.with("/*", StaticServlet.create(eventloop,
								StaticLoaders.ofPath(newCachedThreadPool(), RESOURCE_DIR)));
			}
		});
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RequestParameterExample();
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
