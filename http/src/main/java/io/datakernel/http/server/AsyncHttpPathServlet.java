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

package io.datakernel.http.server;

import io.datakernel.async.ResultCallback;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.exception.HttpException;

import java.util.HashMap;
import java.util.Map;

/**
 * Represent the servlet which contains map of other {@link AsyncHttpServlet} for handling requests from each
 * URL-path. It receives requests and find suitable servlet,it responds to clients across HTTP.
 */
public class AsyncHttpPathServlet implements AsyncHttpServlet {
	private final Map<String, AsyncHttpServlet> handlers = new HashMap<>();

	/**
	 * Handles the received {@link HttpRequest},  creating the {@link HttpResponse} and responds to client with ResultCallback
	 *
	 * @param request  received request
	 * @param callback ResultCallback for handling result
	 */
	@Override
	public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
		AsyncHttpServlet handler = handlers.get(request.getUrl().getPath());
		if (handler != null) {
			handler.serveAsync(request, callback);
		} else {
			callback.onException(new HttpException(404, "Not Found: " + request.getUrl().getPath()));
		}
	}

	/**
	 * Adds the servlet to map of servlets
	 *
	 * @param path    path for using this servlet
	 * @param handler servlet for handling requests from this path
	 */
	public void map(String path, AsyncHttpServlet handler) {
		handlers.put(path, handler);
	}

	/**
	 * Returns the map of servlets
	 */
	public Map<String, AsyncHttpServlet> getMapping() {
		return handlers;
	}
}
