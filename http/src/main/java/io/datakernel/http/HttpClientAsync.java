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

package io.datakernel.http;

import io.datakernel.async.ResultCallback;

/**
 * HttpClientAsync asynchronous sends the {@link HttpRequest} to server and handles received {@link HttpResponse}.
 */
public interface HttpClientAsync {
	/**
	 * Sends a request to the server, receives a response and handle it with callback.
	 *
	 * @param request  request for server
	 * @param timeout  time which client will wait result
	 * @param callback callback for handling result
	 */
	void getHttpResultAsync(HttpRequest request, int timeout, ResultCallback<HttpResponse> callback);

}