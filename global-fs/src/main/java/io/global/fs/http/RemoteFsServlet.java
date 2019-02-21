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
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.remotefs.RemoteFsResponses.FILE_META_CODEC;
import static io.global.fs.api.FsCommand.*;
import static io.global.fs.util.HttpDataFormats.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RemoteFsServlet implements WithMiddleware {
	static final StructuredCodec<List<FileMetadata>> FILE_META_LIST = ofList(FILE_META_CODEC);
	static final StructuredCodec<@Nullable FileMetadata> NULLABLE_FILE_META_CODEC = FILE_META_CODEC.nullable();

	private final MiddlewareServlet servlet;

	private RemoteFsServlet(FsClient client) {
		this.servlet = servlet(client);
	}

	public static RemoteFsServlet create(FsClient client) {
		return new RemoteFsServlet(client);
	}

	private MiddlewareServlet servlet(FsClient client) {
		return MiddlewareServlet.create()
				.with(GET, "/" + LIST, request -> {
					String glob = request.getQueryParameter("glob", "**");
					return (request.getQueryParameterOrNull("tombstones") != null ? client.listEntities(glob) : client.list(glob))
							.thenApplyEx(errorHandler(list ->
									HttpResponse.ok200()
											.withBody(toJson(FILE_META_LIST, list).getBytes(UTF_8))
											.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentTypes.JSON_UTF_8))));
				})
				.with(GET, "/" + GET_METADATA + "/:name*", request -> {
					try {
						return client.getMetadata(request.getPathParameter("name"))
								.thenApplyEx(errorHandler(meta ->
										HttpResponse.ok200()
												.withBody(toJson(NULLABLE_FILE_META_CODEC, meta).getBytes(UTF_8))
												.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentTypes.JSON_UTF_8))));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + UPLOAD, request -> httpUpload(request, client::upload))
				.with(GET, "/" + DOWNLOAD + "/:name*", request -> {
					try {
						String name = request.getPathParameter("name");
						return client.getMetadata(name)
								.thenCompose(meta ->
										meta != null ?
												httpDownload(request, (offset, limit) -> client.download(name, offset, limit), name, meta.getSize()) :
												Promise.ofException(FILE_NOT_FOUND));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + DELETE + "/:name*", request -> {
					try {
						return client.delete(request.getPathParameter("name"), parseRevision(request))
								.thenApplyEx(errorHandler());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + COPY, request -> {
					try {
						return client.copy(request.getQueryParameter("name"), request.getQueryParameter("target"), parseRevision(request))
								.thenApplyEx(errorHandler());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + MOVE, request -> {
					try {
						return client.move(request.getQueryParameter("name"), request.getQueryParameter("target"), parseRevision(request), parseRevision(request, "tombstone"))
								.thenApplyEx(errorHandler());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
