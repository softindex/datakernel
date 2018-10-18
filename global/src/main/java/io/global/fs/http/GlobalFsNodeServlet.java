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
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.util.MemSize;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.GlobalFsMetadata;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.api.GlobalFsPath;
import io.global.fs.transformers.FrameDecoder;
import io.global.fs.transformers.FrameEncoder;
import io.global.ot.util.BinaryDataFormats;

import static io.datakernel.http.AsyncServlet.ensureRequestBody;
import static io.datakernel.http.HttpMethod.*;
import static io.global.fs.util.HttpDataFormats.*;
import static io.global.ot.util.BinaryDataFormats.sizeof;

public final class GlobalFsNodeServlet implements AsyncServlet {
	public static final String UPLOAD = "upload";
	public static final String DOWNLOAD = "download";
	public static final String PUSH = "push";
	public static final String LIST = "list";
	public static final String DEL = "delete";
	public static final String COPY = "copy";
	public static final String MOVE = "move";

	private final AsyncServlet servlet;

	@Inject
	public GlobalFsNodeServlet(GlobalFsNode node) {
		this.servlet = MiddlewareServlet.create()
				.with(GET, "/" + DOWNLOAD + "/:key/:fs/:path*", request -> {
					long[] range = parseRange(request);
					return node.download(parsePath(request), range[0], range[1])
							.thenApply(supplier -> HttpResponse.ok200().withBodyStream(supplier.apply(new FrameEncoder())));
				})
				.with(PUT, "/" + UPLOAD + "/:key/:fs/:path*", request -> {
					long offset = parseOffset(request);
					GlobalFsPath globalPath = parsePath(request);
					SerialSupplier<ByteBuf> body = request.getBodyStream();
					return node.getMetadata(globalPath)
							.thenCompose(meta ->
									node.upload(globalPath, offset)
											.thenCompose(consumer -> body.streamTo(consumer.apply(new FrameDecoder())))
											.thenApply($ -> meta == null ? HttpResponse.ok201() : HttpResponse.ok200()));
				})
				.with(POST, "/" + PUSH + "/:key", ensureRequestBody(MemSize.kilobytes(16), request -> {
					PubKey pubKey = PubKey.fromString(request.getPathParameter("key"));
					SignedData<GlobalFsMetadata> signedMeta = SignedData.ofBytes(request.getBody().asArray(), GlobalFsMetadata::fromBytes);
					return node.pushMetadata(pubKey, signedMeta)
							.thenApply($ -> HttpResponse.ok200());
				}))
				.with(GET, "/" + LIST + "/:key/:fs", request ->
						node.list(parseSpace(request), request.getQueryParameter("glob"))
								.thenApply(list -> HttpResponse.ok200()
										.withBodyStream(SerialSupplier.ofStream(list.stream()
												.map(meta -> {
													byte[] bytes = meta.toBytes();
													ByteBuf buf = ByteBufPool.allocate(sizeof(bytes));
													BinaryDataFormats.writeBytes(buf, bytes);
													return buf;
												})))));
		// .with(POST, "/" + COPY + "/:key/:fs", ensureRequestBody(MemSize.megabytes(1), request ->
		// 		node.copy(parseSpace(request), request.getPostParameters())
		// 				.thenApply(set -> HttpResponse.ok200().withBody(wrapUtf8(STRING_SET.toJson(set))))))
		// .with(POST, "/" + MOVE + "/:key/:fs", ensureRequestBody(MemSize.megabytes(1), request ->
		// 		node.move(parseSpace(request), request.getPostParameters())
		// 				.thenApply(set -> HttpResponse.ok200().withBody(wrapUtf8(STRING_SET.toJson(set))))));
	}

	@Override
	public Stage<HttpResponse> serve(HttpRequest request) throws ParseException, UncheckedException {
		return servlet.serve(request);
	}
}
