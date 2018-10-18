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

package io.global.fs.http;

import io.datakernel.http.*;
import io.datakernel.remotefs.FsClient;

import static io.datakernel.http.AsyncServlet.ensureRequestBody;
import static io.datakernel.http.HttpHeaders.CONTENT_DISPOSITION;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.http.MediaTypes.OCTET_STREAM;
import static io.global.fs.util.HttpDataFormats.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RemoteFsServlet {
	public static final String UPLOAD = "upload";
	public static final String DOWNLOAD = "download";
	public static final String LIST = "list";
	public static final String DEL = "delete";
	public static final String COPY = "copy";
	public static final String MOVE = "move";

	private RemoteFsServlet() {
		throw new AssertionError("nope.");
	}

	public static AsyncServlet wrap(FsClient client) {
		return MiddlewareServlet.create()
				.with(GET, "/" + DOWNLOAD + "/:path*", request -> {
					String path = request.getRelativePath();
					long[] range = parseRange(request);
					String name = path;
					int lastSlash = path.lastIndexOf('/');
					if (lastSlash != -1) {
						name = path.substring(lastSlash + 1);
					}
					return client.download(path, range[0], range[1])
							.thenApply(HttpResponse.ok200()
									.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(OCTET_STREAM)))
									.withHeader(CONTENT_DISPOSITION, HttpHeaderValue.of("attachment; filename=\"" + name + "\""))
									::withBodyStream);
				})
				.with(PUT, "/" + UPLOAD + "/:path*", request -> {
					String path = request.getRelativePath();
					long offset = parseOffset(request);
					return client.getMetadata(path)
							.thenCompose(meta ->
									client.upload(path, offset)
											.thenCompose(request.getBodyStream()::streamTo)
											.thenApply($ -> meta == null ? HttpResponse.ok201() : HttpResponse.ok200()));
				})
				.with(GET, "/" + LIST, request ->
						client.list(request.getQueryParameter("glob"))
								.thenApply(list -> HttpResponse.ok200().withBody(FILE_META_LIST.toJson(list).getBytes(UTF_8))))
				.with(DELETE, "/" + DEL, request ->
						client.delete(request.getQueryParameter("glob"))
								.thenApply($ -> HttpResponse.ok200()))
				.with(POST, "/" + COPY, ensureRequestBody(Integer.MAX_VALUE, request ->
						client.copy(request.getPostParameters())
								.thenApply(set -> HttpResponse.ok200().withBody(STRING_SET.toJson(set).getBytes(UTF_8)))))
				.with(POST, "/" + MOVE, ensureRequestBody(Integer.MAX_VALUE, request ->
						client.move(request.getPostParameters())
								.thenApply(set -> HttpResponse.ok200().withBody(STRING_SET.toJson(set).getBytes(UTF_8)))));
	}
}
