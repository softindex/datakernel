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
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;

public class SimpleServlet implements AsyncServlet {
	private final int workerId;
	private final String responseMessage;

	public SimpleServlet(int id, String responseMessage) {
		this.workerId = id;
		this.responseMessage = responseMessage;
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest httpRequest) {
		byte[] message = encodeAscii("Worker #" + workerId + ". Message: " + responseMessage + "\n");
		return Promise.of(HttpResponse.ok200().withBody(message));
	}
}
