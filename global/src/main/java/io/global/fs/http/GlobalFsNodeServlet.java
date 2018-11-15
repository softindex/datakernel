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
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.GlobalFsMetadata;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FrameDecoder;
import io.global.fs.transformers.FrameEncoder;

import static io.datakernel.http.AsyncServlet.ensureRequestBody;
import static io.datakernel.http.HttpMethod.*;
import static io.global.fs.util.HttpDataFormats.parseOffset;
import static io.global.fs.util.HttpDataFormats.parseRange;
import static io.global.ot.util.BinaryDataFormats2.*;

public final class GlobalFsNodeServlet implements AsyncServlet {
	public static final String UPLOAD = "upload";
	public static final String DOWNLOAD = "download";
	public static final String PUSH = "push";
	public static final String LIST = "list";
	public static final String DEL = "delete";
	public static final String COPY = "copy";
	public static final String MOVE = "move";
	public static final StructuredCodec<SignedData<GlobalFsMetadata>> SIGNED_METADATA_CODEC = REGISTRY.get(new TypeT<SignedData<GlobalFsMetadata>>() {});

	private final AsyncServlet servlet;

	@Inject
	public GlobalFsNodeServlet(GlobalFsNode node) {
		this.servlet = MiddlewareServlet.create()
				.with(GET, "/" + DOWNLOAD + "/:owner/:path*", request -> {
					long[] range = parseRange(request);
					PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
					String path = request.getPathParameter("path");
					return node.download(pubKey, path, range[0], range[1])
							.thenApply(supplier -> HttpResponse.ok200().withBodyStream(supplier.apply(new FrameEncoder())));
				})
				.with(PUT, "/" + UPLOAD + "/:owner/:path*", request -> {
					PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
					String path = request.getPathParameter("path");
					long offset = parseOffset(request);
					SerialSupplier<ByteBuf> body = request.getBodyStream();
					return node.getMetadata(PubKey.fromString(request.getPathParameter("owner")), request.getPathParameter("path"))
							.thenCompose(meta ->
									node.upload(pubKey, path, offset)
											.thenCompose(consumer -> body.streamTo(consumer.apply(new FrameDecoder())))
											.thenApply($ -> meta == null ? HttpResponse.ok201() : HttpResponse.ok200()));
				})
				.with(POST, "/" + PUSH + "/:owner", ensureRequestBody(request -> {
					PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
					SignedData<GlobalFsMetadata> signedMeta = decode(SIGNED_METADATA_CODEC, request.getBody());
					return node.pushMetadata(pubKey, signedMeta)
							.thenApply($ -> HttpResponse.ok200());
				}))
				.with(GET, "/" + LIST + "/:owner/:name", request -> {
					PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
					return (request.getQueryParameter("local", "").isEmpty() ?
							node.list(pubKey, request.getQueryParameter("glob")) :
							node.listLocal(pubKey, request.getQueryParameter("glob")))
							.thenApply(list -> HttpResponse.ok200()
									.withBodyStream(
											SerialSupplier.ofStream(
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
