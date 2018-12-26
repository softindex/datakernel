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

package io.global.ot.demo.client;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.global.ot.demo.api.OTStateServlet;
import io.global.ot.demo.state.StateManagerProvider;

import java.nio.file.Path;
import java.nio.file.Paths;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static java.util.concurrent.Executors.newCachedThreadPool;

class HttpModule extends AbstractModule {
	private static final String DEFAULT_PATH_TO_RESOURCES = "src/main/resources/static";

	@Provides
	@Singleton
	IAsyncHttpClient provideClient(Eventloop eventloop) {
		return AsyncHttpClient.create(eventloop);
	}

	@Provides
	@Singleton
	AsyncHttpServer provide(Eventloop eventloop, AsyncServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.withListenPort(config.get(ofInteger(), "http.listenPort"));
	}

	@Provides
	@Singleton
	AsyncServlet provideMainServlet(Eventloop eventloop, StateManagerProvider stateManagerProvider) {
		Path resources = Paths.get(DEFAULT_PATH_TO_RESOURCES);
		StaticLoader resourceLoader = StaticLoaders.ofPath(newCachedThreadPool(), resources);
		StaticServlet staticServlet = StaticServlet.create(eventloop, resourceLoader);
		return OTStateServlet.create(stateManagerProvider).getMiddlewareServlet()
				.withFallback(staticServlet);
	}
}
