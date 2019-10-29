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

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.parse.ParseException;
import io.datakernel.http.*;
import io.datakernel.promise.Promise;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import org.jetbrains.annotations.NotNull;
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

public final class RemoteFsServlet implements AsyncServlet {
	static final StructuredCodec<List<FileMetadata>> FILE_META_LIST = ofList(FILE_META_CODEC);
	static final StructuredCodec<@Nullable FileMetadata> NULLABLE_FILE_META_CODEC = FILE_META_CODEC.nullable();

	private final RoutingServlet servlet;

	private RemoteFsServlet(FsClient client) {
		servlet = servlet(client);
	}

	public static RemoteFsServlet create(FsClient client) {
		return new RemoteFsServlet(client);
	}

	private RoutingServlet servlet(FsClient client) {
		return RoutingServlet.create()
				.map(GET, "/" + LIST, request -> {
					String glob = request.getQueryParameter("glob");
					glob = glob != null ? glob : "**";
					return (request.getQueryParameter("tombstones") != null ? client.listEntities(glob) : client.list(glob))
							.mapEx(errorHandler(list ->
									HttpResponse.ok200()
											.withBody(toJson(FILE_META_LIST, list).getBytes(UTF_8))
											.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentTypes.JSON_UTF_8))));
				})
				.map(GET, "/" + GET_METADATA + "/*", request ->
						client.getMetadata(request.getRelativePath())
								.mapEx(errorHandler(meta ->
										HttpResponse.ok200()
												.withBody(toJson(NULLABLE_FILE_META_CODEC, meta).getBytes(UTF_8))
												.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentTypes.JSON_UTF_8)))))
				.map(POST, "/" + UPLOAD, request -> httpUpload(request, client::upload))
				.map(GET, "/" + DOWNLOAD + "/*", request -> {
					String name = request.getRelativePath();
					return client.getMetadata(name)
							.then(meta -> {
								if (meta == null) {
									return Promise.<HttpResponse>ofException(FILE_NOT_FOUND);
								}
								return HttpResponse.file(
										(offset, limit) -> client.download(name, offset, limit),
										name,
										meta.getSize(),
										request.getHeader(HttpHeaders.RANGE));
							});
				})
				.map(POST, "/" + DELETE + "/*", request -> {
					try {
						return client.delete(request.getRelativePath(), parseRevision(request))
								.mapEx(errorHandler());
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				})
				.map(POST, "/" + COPY, request -> {
					String name = request.getQueryParameter("name");
					String target = request.getQueryParameter("target");
					if (name == null) {
						return Promise.ofException(HttpException.ofCode(400, "No 'name' query parameter"));
					}
					if (target == null) {
						return Promise.ofException(HttpException.ofCode(400, "No 'target' query parameter"));
					}
					try {
						return client.copy(name, target, parseRevision(request))
								.mapEx(errorHandler());
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				})
				.map(POST, "/" + MOVE, request -> {
					String name = request.getQueryParameter("name");
					String target = request.getQueryParameter("target");
					if (name == null) {
						return Promise.ofException(HttpException.ofCode(400, "No 'name' query parameter"));
					}
					if (target == null) {
						return Promise.ofException(HttpException.ofCode(400, "No 'target' query parameter"));
					}
					try {
						return client.move(name, target, parseRevision(request), parseRevision(request, "tombstone"))
								.mapEx(errorHandler());
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				});
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return servlet.serve(request);
	}
}
