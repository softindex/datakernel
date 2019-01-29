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

package io.global.fs.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.global.fs.util.HttpDataFormats;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.http.MediaTypes.OCTET_STREAM;
import static io.datakernel.remotefs.RemoteFsResponses.FILE_META_CODEC;
import static io.global.fs.api.FsCommand.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RemoteFsServlet implements WithMiddleware {
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
				.with(GET, "/" + LIST, request ->
						client.list(request.getQueryParameter("glob", "**"))
								.thenApply(list -> HttpResponse.ok200()
										.withBody(toJson(FILE_META_LIST, list).getBytes(UTF_8))
										.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(JSON)))))
				.with(POST, "/" + UPLOAD, request -> {
					try {
						long offset = HttpDataFormats.parseOffset(request);
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
						String contentType = request.getHeader(CONTENT_TYPE);
						if (!contentType.startsWith("multipart/form-data; boundary=")) {
							return Promise.ofException(HttpException.ofCode(400, "Content type is not multipart/form-data"));
						}
						String boundary = contentType.substring(30);
						if (boundary.startsWith("\"") && boundary.endsWith("\"")) {
							boundary = boundary.substring(1, boundary.length() - 1);
						}
						return MultipartParser.create(boundary)
								.splitByFiles(bodyStream, upload -> ChannelConsumer.ofPromise(client.upload(upload, offset)))
								.thenApply($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + DOWNLOAD + "/:path*", request -> {
					try {
						String path = request.getPathParameter("path");
						int lastSlash = path.lastIndexOf('/');
						String name = lastSlash != -1 ? path.substring(lastSlash + 1) : path;
						String headerRange = request.getHeaderOrNull(HttpHeaders.RANGE);
						if (headerRange == null) {
							return client.getMetadata(path)
									.thenCompose(meta -> {
										if (meta == null) {
											return Promise.<HttpResponse>ofException(HttpException.ofCode(404, "File '" + path + "' not found"));
										}
										return client.download(path, 0, -1)
												.thenApply(HttpResponse.ok200()
														.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(OCTET_STREAM)))
														.withHeader(CONTENT_DISPOSITION, "attachment; filename=\"" + name + "\"")
														.withHeader(ACCEPT_RANGES, "bytes")
														.withHeader(CONTENT_LENGTH, Long.toString(meta.getSize()))
														::withBodyStream);
									});
						}
						if (!headerRange.startsWith("bytes=")) {
							throw HttpException.ofCode(416, "Invalid range header (not in bytes)");
						}
						headerRange = headerRange.substring(6);
						if (!headerRange.matches("(\\d+)?-(\\d+)?")) {
							throw HttpException.ofCode(416, "Only single part ranges are allowed");
						}
						String[] parts = headerRange.split("-", 2);
						long offset = parts[0].isEmpty() ? 0 : Long.parseLong(parts[0]);
						long endOffset = parts[1].isEmpty() ? -1 : Long.parseLong(parts[1]);
						if (endOffset != -1 && offset > endOffset) {
							throw HttpException.ofCode(416, "Invalid range");
						}
						return client.getMetadata(path)
								.thenCompose(meta -> {
									if (meta == null) {
										return Promise.<HttpResponse>ofException(HttpException.ofCode(404, "File '" + path + "' not found"));
									}
									long length = (endOffset == -1 ? meta.getSize() : endOffset) - offset + 1;
									return client.download(path, offset, length)
											.thenApply(HttpResponse.ok206()
													.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(OCTET_STREAM)))
													.withHeader(CONTENT_DISPOSITION, HttpHeaderValue.of("attachment; filename=\"" + name + "\""))
													.withHeader(ACCEPT_RANGES, "bytes")
													.withHeader(CONTENT_RANGE, offset + "-" + (offset + length) + "/" + meta.getSize())
													.withHeader(CONTENT_LENGTH, "" + length)
													::withBodyStream);
								});
					} catch (ParseException | HttpException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + DELETE, request -> {
					try {
						return client.deleteBulk(request.getQueryParameter("glob"))
								.thenApply($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + DELETE + "/:path*", request -> {
					try {
						String path = request.getPathParameter("path");
						return client.delete(path)
								.thenApply($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + COPY, request -> request.getPostParameters()
						.thenCompose(postParameters -> client.copyBulk(postParameters)
								.thenApply($ -> HttpResponse.ok200())))
				.with(POST, "/" + MOVE, request -> request.getPostParameters()
						.thenCompose(postParameters -> client.moveBulk(postParameters)
								.thenApply($ -> HttpResponse.ok200())));
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
