/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.loader.StaticLoader;
import org.jetbrains.annotations.NotNull;

import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.StaticServlet.METHOD_NOT_ALLOWED;

public final class SingleResourceStaticServlet implements AsyncServlet {
	private final Eventloop eventloop;
	private final StaticLoader resourceLoader;
	private final String path;

	private int responseCode = 200;
	private ContentType contentType;

	private SingleResourceStaticServlet(Eventloop eventloop, StaticLoader resourceLoader, String path, ContentType contentType) {
		this.eventloop = eventloop;
		this.resourceLoader = resourceLoader;
		this.path = path;
		this.contentType = contentType;
	}

	public static SingleResourceStaticServlet create(Eventloop eventloop, StaticLoader resourceLoader, String path) {
		return new SingleResourceStaticServlet(eventloop, resourceLoader, path, StaticServlet.getContentType(path));
	}

	public static SingleResourceStaticServlet create(Eventloop eventloop, ByteBuf data, ContentType contentType) {
		return new SingleResourceStaticServlet(eventloop, $ -> Promise.of(data), "", contentType);
	}

	public static SingleResourceStaticServlet create(Eventloop eventloop, byte[] data, ContentType contentType) {
		return create(eventloop, ByteBuf.wrapForReading(data), contentType);
	}

	public SingleResourceStaticServlet withResponseCode(int responseCode) {
		this.responseCode = responseCode;
		return this;
	}

	public SingleResourceStaticServlet withContentType(ContentType contentType) {
		this.contentType = contentType;
		return this;
	}

	@NotNull
	@Override
	public final Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		assert eventloop.inEventloopThread();
		if (request.getMethod() != HttpMethod.GET) {
			return Promise.ofException(METHOD_NOT_ALLOWED);
		}
		return resourceLoader.getResource(path)
				.map(buf ->
						HttpResponse.ofCode(responseCode)
								.withBody(buf)
								.withHeader(CONTENT_TYPE, ofContentType(contentType)));
	}
}
