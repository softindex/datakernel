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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.loader.StaticLoader;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;

public final class StaticServlet implements AsyncServlet {
	public static final Charset DEFAULT_TXT_ENCODING = StandardCharsets.UTF_8;
	public static final String DEFAULT_INDEX_FILE_NAME = "index.html"; // response for get request asking for root
	public static final HttpException BAD_PATH_ERROR = HttpException.ofCode(400, "Bad path and query section");
	public static final HttpException METHOD_NOT_ALLOWED = HttpException.ofCode(405, "Only GET is being allowed");

	private final Eventloop eventloop;
	private final StaticLoader resourceLoader;

	private StaticServlet(Eventloop eventloop, StaticLoader resourceLoader) {
		this.eventloop = eventloop;
		this.resourceLoader = resourceLoader;
	}

	public static StaticServlet create(Eventloop eventloop, StaticLoader resourceLoader) {
		return new StaticServlet(eventloop, resourceLoader);
	}

	static ContentType getContentType(String path) {
		int pos = path.lastIndexOf(".");
		if (pos != -1) {
			path = path.substring(pos + 1);
		}
		MediaType mime = MediaTypes.getByExtension(path);
		if (mime == null) {
			mime = MediaTypes.OCTET_STREAM;
		}
		ContentType type;
		if (mime.isTextType()) {
			type = ContentType.of(mime, DEFAULT_TXT_ENCODING);
		} else {
			type = ContentType.of(mime);
		}
		return type;
	}

	private HttpResponse createHttpResponse(ByteBuf buf, String path) {
		return HttpResponse.ofCode(200)
				.withBody(buf)
				.withHeader(CONTENT_TYPE, ofContentType(getContentType(path)));
	}

	@NotNull
	@Override
	public final Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		assert eventloop.inEventloopThread();

		String path = request.getRelativePath();

		if (request.getMethod() != HttpMethod.GET) return Promise.ofException(METHOD_NOT_ALLOWED);

		if (path.equals("")) {
			path = DEFAULT_INDEX_FILE_NAME;
		}
		String finalPath = path;

		return resourceLoader.getResource(path).thenApply(byteBuf -> createHttpResponse(byteBuf, finalPath));
	}
}
