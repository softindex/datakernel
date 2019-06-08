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
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.HttpMethod.GET;

public final class RoutingServletExample extends HttpServerLauncher {
	@Provides
	AsyncServlet servlet() {
		return RoutingServlet.create()
				.with(GET, "/", request -> Promise.of(HttpResponse.ok200()
						.withBody(wrapUtf8("<h1>Go to some pages</h1>" +
								"<a href=\"/path1\"> Path 1 </a><br>" +
								"<a href=\"/path2\"> Path 2 </a>"))))
				.with(GET, "/path1", request -> Promise.of(HttpResponse.ok200()
						.withBody(wrapUtf8("<h1>Hello form the first path!</h1>" +
								"<a href=\"/\">Go home</a>"))))
				.with(GET, "/path2", request -> Promise.of(HttpResponse.ok200()
						.withBody(wrapUtf8("<h1>Hello from the second path!</h1>" +
								"<a href=\"/\">Go home</a>"))))
				.with("/*", request -> Promise.of(HttpResponse.ofCode(404)
						.withBody(wrapUtf8("<h1>404</h1><p>Path '" + request.getRelativePath() + "' not found</p>" +
								"<a href=\"/\">Go home</a>"))));
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RoutingServletExample();
		launcher.launch(args);
	}
}
