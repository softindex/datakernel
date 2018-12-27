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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.global.fs.util.HttpDataFormats;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.remotefs.RemoteFsResponses.FILE_META_CODEC;
import static io.global.fs.api.FsCommand.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RemoteFsServlet implements WithMiddleware {
	//	static final StructuredCodec<Set<String>> STRING_SET = ofSet(STRING_CODEC);
	static final StructuredCodec<List<FileMetadata>> FILE_META_LIST = ofList(FILE_META_CODEC);

	private final MiddlewareServlet servlet;

	private RemoteFsServlet(FsClient client) {
		this.servlet = servlet(client);
	}

	public static RemoteFsServlet create(FsClient client) {
		return new RemoteFsServlet(client);
	}


	private MiddlewareServlet servlet(FsClient client) {
		return MiddlewareServlet.create()
				.with(HttpMethod.POST, "/" + UPLOAD + "/:path*", request -> {
					try {
						String path = request.getPathParameter("path");
						long offset = HttpDataFormats.parseOffset(request);
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();

						String contentType = request.getHeader(HttpHeaders.CONTENT_TYPE);
						if (path.isEmpty() && !contentType.startsWith("multipart/form-data; boundary=")) {
							return Promise.ofException(HttpException.ofCode(400, "Path is empty and content type is not multipart/form-data"));
						}
						String boundary = contentType.substring(30);
						if (boundary.startsWith("\"") && boundary.endsWith("\"")) {
							boundary = boundary.substring(1, boundary.length() - 1);
						}

						String finalBoundary = boundary;

						return (offset == -1 || offset == 0) && path.isEmpty() ?
								MultipartParser.create(finalBoundary)
										.splitByFiles(bodyStream, filename -> ChannelConsumer.ofPromise(client.upload(filename, offset)))
										.thenApply($ -> HttpResponse.ok201()) :
								client.getMetadata(path)
										.thenCompose(meta ->
												client.upload(path, offset)
														.thenCompose(BinaryChannelSupplier.of(bodyStream).parseStream(MultipartParser.create(finalBoundary).ignoreHeaders())::streamTo)
														.thenApply($ -> meta == null ? HttpResponse.ok201() : HttpResponse.ok200()));

					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(HttpMethod.GET, "/" + DOWNLOAD + "/:path*", request -> {
					try {
						String path = request.getPathParameter("path");
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
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(HttpMethod.GET, "/" + LIST, request ->
						client.list(request.getQueryParameter("glob", "**"))
								.thenApply(list -> HttpResponse.ok200()
										.withBody(toJson(FILE_META_LIST, list).getBytes(UTF_8))
										.withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.JSON)))))
				.with(HttpMethod.GET, "/" + DELETE, request -> {
					try {
						return client.deleteBulk(request.getQueryParameter("glob"))
								.thenApply($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(HttpMethod.GET, "/" + COPY, request -> request.getPostParameters()
						.thenCompose(postParameters -> client.copyBulk(postParameters)
								.thenApply($ -> HttpResponse.ok200())))
				.with(HttpMethod.GET, "/" + MOVE, request -> request.getPostParameters()
						.thenCompose(postParameters -> client.moveBulk(postParameters)
								.thenApply($ -> HttpResponse.ok200())));
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
