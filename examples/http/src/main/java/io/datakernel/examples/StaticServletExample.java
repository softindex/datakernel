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

import io.datakernel.di.module.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import static io.datakernel.loader.StaticLoader.ofClassPath;

public final class StaticServletExample extends HttpServerLauncher {
	@Provides
	AsyncServlet servlet() {
		return StaticServlet.create(ofClassPath("static/site"))
				.withMappingEmptyTo("index.html");
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new StaticServletExample();
		launcher.launch(args);
	}
}
