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

import com.google.inject.Inject;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.GlobalFsMetadata;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FrameDecoder;
import io.global.fs.transformers.FrameEncoder;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeWithSizePrefix;
import static io.datakernel.http.AsyncServlet.ensureRequestBody;
import static io.datakernel.http.HttpMethod.*;
import static io.global.fs.api.MetadataStorage.NO_METADATA;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static io.global.fs.util.HttpDataFormats.parseOffset;
import static io.global.fs.util.HttpDataFormats.parseRange;

public final class GlobalFsNodeServlet implements AsyncServlet {
	static final String UPLOAD = "upload";
	static final String DOWNLOAD = "download";
	static final String PUSH = "push";
	static final String LIST = "list";
	// static final String COPY = "copy";
	// static final String MOVE = "move";

	static final StructuredCodec<SignedData<GlobalFsMetadata>> SIGNED_METADATA_CODEC = REGISTRY.get(new TypeT<SignedData<GlobalFsMetadata>>() {});

	private final AsyncServlet servlet;

	@Inject
	public GlobalFsNodeServlet(GlobalFsNode node) {
		this.servlet = MiddlewareServlet.create()
				.with(GET, "/" + DOWNLOAD + "/:owner/:path*", request -> {
					long[] range = parseRange(request);
					PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
					String path = request.getPathParameter("path");
					return node.download(pubKey, path, range[0], range[1])
							.thenApply(supplier ->
									HttpResponse.ok200()
											.withBodyStream(supplier.transformWith(new FrameEncoder())));
				})
				.with(PUT, "/" + UPLOAD + "/:owner/:path*", request -> {
					PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
					String path = request.getPathParameter("path");
					long offset = parseOffset(request);
					ChannelSupplier<ByteBuf> body = request.getBodyStream();
					return node.getMetadata(PubKey.fromString(request.getPathParameter("owner")), request.getPathParameter("path"))
							.thenComposeEx((meta, e) -> {
								boolean newFile = e == NO_METADATA;
								if (e == null || newFile) {
									return node.upload(pubKey, path, offset)
											.thenCompose(consumer -> body.streamTo(consumer.transformWith(new FrameDecoder())))
											.thenApply($ -> newFile ? HttpResponse.ok201() : HttpResponse.ok200());
								}
								return Promise.ofException(e);
							});
				})
				.with(POST, "/" + PUSH + "/:owner", ensureRequestBody(request -> {
					PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
					SignedData<GlobalFsMetadata> signedMeta = decode(SIGNED_METADATA_CODEC, request.getBody());
					return node.pushMetadata(pubKey, signedMeta)
							.thenApply($ -> HttpResponse.ok200());
				}))
				.with(GET, "/" + LIST + "/:owner/:name", request -> {
					PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
					return node.list(pubKey, request.getQueryParameter("glob"))
							.thenApply(list -> HttpResponse.ok200()
									.withBodyStream(
											ChannelSupplier.ofStream(
													list.stream()
															.map(meta -> encodeWithSizePrefix(SIGNED_METADATA_CODEC, meta)))));
				});
		// .with(POST, "/" + COPY + "/:owner/:fs", ensureRequestBody(MemSize.megabytes(1), request ->
		// 		node.copy(parseNamespace(request), request.getPostParameters())
		// 				.thenApply(set -> HttpResponse.ok200().withBody(wrapUtf8(STRING_SET.toJson(set))))))
		// .with(POST, "/" + MOVE + "/:owner/:fs", ensureRequestBody(MemSize.megabytes(1), request ->
		// 		node.move(parseNamespace(request), request.getPostParameters())
		// 				.thenApply(set -> HttpResponse.ok200().withBody(wrapUtf8(STRING_SET.toJson(set))))));
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws ParseException, UncheckedException {
		return servlet.serve(request);
	}
}
