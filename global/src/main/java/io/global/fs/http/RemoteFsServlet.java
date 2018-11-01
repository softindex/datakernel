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

import com.google.gson.TypeAdapter;
import com.google.inject.Inject;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.*;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialSupplier;
import io.global.fs.util.HttpDataFormats;

import java.util.List;
import java.util.Set;

import static io.datakernel.json.GsonAdapters.*;
import static io.datakernel.remotefs.RemoteFsResponses.FILE_META_JSON;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RemoteFsServlet implements AsyncServlet {
	public static final String UPLOAD = "upload";
	public static final String DOWNLOAD = "download";
	public static final String LIST = "list";
	public static final String DEL = "delete";
	public static final String COPY = "copy";
	public static final String MOVE = "move";

	static final TypeAdapter<Set<String>> STRING_SET = ofSet(STRING_JSON);
	static final TypeAdapter<List<FileMetadata>> FILE_META_LIST = ofList(FILE_META_JSON);

	private final AsyncServlet servlet;

	@Inject
	public RemoteFsServlet(FsClient client) {
		servlet = MiddlewareServlet.create()
				.with(HttpMethod.GET, "/" + DOWNLOAD + "/:path*", request -> {
					String path = request.getRelativePath();
					long[] range = HttpDataFormats.parseRange(request);
					String name = path;
					int lastSlash = path.lastIndexOf('/');
					if (lastSlash != -1) {
						name = path.substring(lastSlash + 1);
					}
					return client.download(path, range[0], range[1])
							.thenApply(HttpResponse.ok200()
									.withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.OCTET_STREAM)))
									.withHeader(HttpHeaders.CONTENT_DISPOSITION, HttpHeaderValue.of("attachment; filename=\"" + name + "\""))
									::withBodyStream);
				})
				.with(HttpMethod.PUT, "/" + UPLOAD + "/:path*", request -> {
					String path = request.getRelativePath();
					long offset = HttpDataFormats.parseOffset(request);
					SerialSupplier<ByteBuf> bodyStream = request.getBodyStream();
					return client.getMetadata(path)
							.thenCompose(meta ->
									client.upload(path, offset)
											.thenCompose(bodyStream::streamTo)
											.thenApply($ -> meta == null ? HttpResponse.ok201() : HttpResponse.ok200()));
				})
				.with(HttpMethod.GET, "/" + LIST, request ->
						client.list(request.getQueryParameter("glob"))
								.thenApply(list -> HttpResponse.ok200()
										.withBody(FILE_META_LIST.toJson(list).getBytes(UTF_8))
										.withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.JSON)))))
				.with(HttpMethod.DELETE, "/" + DEL, request ->
						client.delete(request.getQueryParameter("glob"))
								.thenApply($ -> HttpResponse.ok200()))
				.with(HttpMethod.POST, "/" + COPY, AsyncServlet.ensureRequestBody(Integer.MAX_VALUE, request ->
						client.copy(request.getPostParameters())
								.thenApply(set -> HttpResponse.ok200()
										.withBody(HttpDataFormats.STRING_SET.toJson(set).getBytes(UTF_8))
										.withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.JSON))))))
				.with(HttpMethod.POST, "/" + MOVE, AsyncServlet.ensureRequestBody(Integer.MAX_VALUE, request ->
						client.move(request.getPostParameters())
								.thenApply(set -> HttpResponse.ok200()
										.withBody(HttpDataFormats.STRING_SET.toJson(set).getBytes(UTF_8))
										.withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.JSON))))));
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws ParseException, UncheckedException {
		return servlet.serve(request);
	}
}
