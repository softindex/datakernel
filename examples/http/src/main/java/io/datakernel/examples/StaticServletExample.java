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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import static io.datakernel.loader.StaticLoaders.ofPath;
import static io.datakernel.util.CollectionUtils.list;
import static java.lang.Boolean.parseBoolean;

public final class StaticServletExample extends HttpServerLauncher {
	private static final Path RESOURCE_DIR = Paths.get("src/main/resources/static/site");

	@Override
	protected Collection<Module> getBusinessLogicModules() {
		return list(new AbstractModule() {
			@Provides
			@Singleton
			AsyncServlet staticServlet(Eventloop eventloop) {
				return StaticServlet.create(eventloop, ofPath(RESOURCE_DIR));
			}
		});
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new StaticServletExample();
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
