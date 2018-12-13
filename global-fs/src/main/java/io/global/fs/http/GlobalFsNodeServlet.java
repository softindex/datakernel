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
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.WithMiddleware;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FrameDecoder;
import io.global.fs.transformers.FrameEncoder;

import static io.datakernel.codec.binary.BinaryUtils.*;
import static io.datakernel.http.AsyncServlet.ensureRequestBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.fs.api.CheckpointStorage.NO_CHECKPOINT;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static io.global.fs.util.HttpDataFormats.parseOffset;
import static io.global.fs.util.HttpDataFormats.parseRange;

public final class GlobalFsNodeServlet implements WithMiddleware {
	static final String UPLOAD = "upload";
	static final String DOWNLOAD = "download";
	static final String LIST = "list";
	static final String GET_METADATA = "getMetadata";
	static final String DELETE = "delete";
	// static final String COPY = "copy";
	// static final String MOVE = "move";

	static final StructuredCodec<SignedData<GlobalFsCheckpoint>> SIGNED_CHECKPOINT_CODEC = REGISTRY.get(new TypeT<SignedData<GlobalFsCheckpoint>>() {});

	private final MiddlewareServlet servlet;

	private GlobalFsNodeServlet(GlobalFsNode node) {
		this.servlet = servlet(node);
	}

	public static GlobalFsNodeServlet create(GlobalFsNode node) {
		return new GlobalFsNodeServlet(node);
	}

	private static MiddlewareServlet servlet(GlobalFsNode node) {
		return MiddlewareServlet.create()
				.with(POST, "/" + UPLOAD + "/:space/:path*", request -> {
					PubKey space = PubKey.fromString(request.getPathParameter("space"));
					String path = request.getPathParameter("path");
					long offset = parseOffset(request);
					ChannelSupplier<ByteBuf> body = request.getBodyStream();
					return node.getMetadata(space, path)
							.thenComposeEx((meta, e) -> {
								boolean newFile = e == NO_CHECKPOINT;
								if (e == null || newFile) {
									return node.upload(space, path, offset)
											.thenCompose(consumer -> body.streamTo(consumer.transformWith(new FrameDecoder())))
											.thenApply($ -> newFile ? HttpResponse.ok201() : HttpResponse.ok200());
								}
								return Promise.ofException(e);
							});
				})
				.with(GET, "/" + DOWNLOAD + "/:space/:path*", request -> {
					long[] range = parseRange(request);
					PubKey space = PubKey.fromString(request.getPathParameter("space"));
					String path = request.getPathParameter("path");
					return node.download(space, path, range[0], range[1])
							.thenApply(supplier ->
									HttpResponse.ok200()
											.withBodyStream(supplier.transformWith(new FrameEncoder())));
				})
				.with(GET, "/" + LIST + "/:space/:name", request -> {
					PubKey space = PubKey.fromString(request.getPathParameter("space"));
					return node.list(space, request.getQueryParameter("glob"))
							.thenApply(list -> HttpResponse.ok200()
									.withBodyStream(
											ChannelSupplier.ofStream(list
													.stream()
													.map(meta ->
															encodeWithSizePrefix(SIGNED_CHECKPOINT_CODEC, meta)))));
				})
				.with(GET, "/" + GET_METADATA + "/:space/:path*", request -> {
					PubKey space = PubKey.fromString(request.getPathParameter("space"));
					return node.getMetadata(space, request.getPathParameter("path"))
							.thenComposeEx((meta, e) -> {
								if (e == null) {
									return Promise.of(HttpResponse.ok200()
											.withBody(encode(SIGNED_CHECKPOINT_CODEC, meta)));
								}
								if (e == NO_CHECKPOINT) {
									return Promise.of(HttpResponse.ofCode(404));
								}
								return Promise.ofException(e);
							});
				})
				.with(POST, "/" + DELETE + "/:space", ensureRequestBody(request -> {
					PubKey space = PubKey.fromString(request.getPathParameter("space"));
					SignedData<GlobalFsCheckpoint> checkpoint = decode(SIGNED_CHECKPOINT_CODEC, request.takeBody());
					return node.delete(space, checkpoint)
							.thenApply(list -> HttpResponse.ok200());
				}));
		// .with(POST, "/" + COPY + "/:space/:fs", ensureRequestBody(MemSize.megabytes(1), request ->
		// 		node.copy(parseNamespace(request), request.getPostParameters())
		// 				.thenApply(set -> HttpResponse.ok200().withBody(wrapUtf8(STRING_SET.toJson(set))))))
		// .with(POST, "/" + MOVE + "/:space/:fs", ensureRequestBody(MemSize.megabytes(1), request ->
		// 		node.move(parseNamespace(request), request.getPostParameters())
		// 				.thenApply(set -> HttpResponse.ok200().withBody(wrapUtf8(STRING_SET.toJson(set))))));
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
