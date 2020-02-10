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

package io.datakernel.http;

import io.datakernel.common.exception.UncheckedException;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;

import static io.datakernel.eventloop.RunnableWithContext.wrapContext;

/**
 * A stub client which forwards requests straight to the underlying servlet without any real I/O operations.
 * Used for testing.
 */
public final class StubHttpClient implements IAsyncHttpClient {
	private final AsyncServlet servlet;

	private StubHttpClient(AsyncServlet servlet) {
		this.servlet = servlet;
	}

	public static StubHttpClient of(AsyncServlet servlet) {
		return new StubHttpClient(servlet);
	}

	// this piece of error formatting is stolen directly from HttpServerConnection
	@Override
	public Promise<HttpResponse> request(HttpRequest request) {
		Promise<HttpResponse> servletResult;
		try {
			servletResult = servlet.serveAsync(request);
		} catch (UncheckedException u) {
			servletResult = Promise.ofException(u.getCause());
		}
		return servletResult.thenEx((res, e) -> {
			request.recycle();
			if (e == null) {
				Eventloop.getCurrentEventloop().post(wrapContext(res, res::recycle));
				return Promise.of(res);
			} else {
				return Promise.ofException(e);
			}
		});
	}
}
