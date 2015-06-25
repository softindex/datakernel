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

/**
 * Servlet receives and responds to {@link HttpRequest} from clients across HTTP.
 * Receives {@link HttpRequest},  creates {@link HttpResponse} and sends it.
 */
public interface AsyncHttpServlet {
	/**
	 * Handles the received {@link HttpRequest},  creating the {@link HttpResponse} and responds to client with ResultCallback
	 *
	 * @param request  received request
	 * @param callback ResultCallback for handling result
	 */
	void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback);
}
