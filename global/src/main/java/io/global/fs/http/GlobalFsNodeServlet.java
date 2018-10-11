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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.serial.SerialSupplier;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.api.GlobalFsPath;
import io.global.fs.transformers.FrameDecoder;
import io.global.fs.transformers.FrameEncoder;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.PUT;
import static io.global.fs.util.HttpDataFormats.*;

public final class GlobalFsNodeServlet {

	public static final String UPLOAD = "upload";
	public static final String DOWNLOAD = "download";
	public static final String LIST = "list";
	public static final String DEL = "delete";
	public static final String COPY = "copy";
	public static final String MOVE = "move";

	private GlobalFsNodeServlet() {
		throw new AssertionError("nope.");
	}

	public static AsyncServlet wrap(GlobalFsNode node) {
		return MiddlewareServlet.create()
				.with(GET, "/" + DOWNLOAD + "/:key/:fs",
						MiddlewareServlet.create().withFallback(request -> {
							long[] range = parseRange(request);
							return node.download(parsePath(request), range[0], range[1])
									.thenApply(supplier -> HttpResponse.ok200().withBodyStream(supplier.apply(new FrameEncoder())));
						}))
				.with(PUT, "/" + UPLOAD + "/:key/:fs",
						MiddlewareServlet.create().withFallback(request -> {
							long offset = parseOffset(request);
							GlobalFsPath globalPath = parsePath(request);
							SerialSupplier<ByteBuf> body = request.getBodyStream();
							return node.getMetadata(globalPath)
									.thenCompose(meta ->
											node.upload(globalPath, offset)
													.thenCompose(consumer -> body.streamTo(consumer.apply(new FrameDecoder())))
													.thenApply($ -> meta == null ? HttpResponse.ok201() : HttpResponse.ok200()));
						}));
		// TODO anton: fix reimplement this
		// .with(GET, "/" + LIST + "/:key/:fs", request ->
		// 		node.list(parseName(request), request.getQueryParameter("glob"))
		// 				.thenApply(list -> HttpResponse.ok200()
		// 						.withBody(wrapUtf8(KEYLESS_META_LIST.toJson(list.stream()
		// 								.map(KeylessGlobalFsMetadata::from)
		// 								.collect(toList()))))))
		// .with(DELETE, "/" + DEL, request ->
		// 		node.delete(SignedData.ofBytes(request.getBody().asArray(), DeletionRequest::fromBytes))
		// 				.thenApply($ -> HttpResponse.ok200()));
		// .with(POST, "/" + COPY + "/:key/:fs", ensureRequestBody(MemSize.megabytes(1), request ->
		// 		node.copy(parseName(request), request.getPostParameters())
		// 				.thenApply(set -> HttpResponse.ok200().withBody(wrapUtf8(STRING_SET.toJson(set))))))
		// .with(POST, "/" + MOVE + "/:key/:fs", ensureRequestBody(MemSize.megabytes(1), request ->
		// 		node.move(parseName(request), request.getPostParameters())
		// 				.thenApply(set -> HttpResponse.ok200().withBody(wrapUtf8(STRING_SET.toJson(set))))));
	}
}
