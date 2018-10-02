/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalfs.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.serial.SerialSupplier;
import io.global.common.PubKey;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsNode;
import io.global.globalfs.api.GlobalFsPath;
import io.global.globalfs.transformers.FrameDecoder;
import io.global.globalfs.transformers.FrameEncoder;
import io.global.globalsync.util.BinaryDataFormats;

import java.util.Map;

import static io.datakernel.http.AsyncServlet.ensureBody;
import static io.datakernel.http.HttpMethod.*;
import static io.global.globalsync.util.BinaryDataFormats.sizeof;

public final class GlobalFsNodeServlet {
	public static final String UPLOAD = "upload";
	public static final String DOWNLOAD = "download";
	public static final String LIST = "list";
	public static final String DEL = "delete";
	public static final String COPY = "copy";
	public static final String MOVE = "move";

	// region creators
	private GlobalFsNodeServlet() {
		throw new AssertionError("nope.");
	}

	public static AsyncServlet wrap(GlobalFsNode node) {
		return MiddlewareServlet.create()
				.with(GET, "/" + DOWNLOAD, request -> {
					PubKey pubKey = PubKey.fromString(request.getQueryParameter("key"));
					String fs = request.getQueryParameter("fs");
					String path = request.getQueryParameter("path");
					long offset = parseLong(request.getQueryParameter("offset"));
					long limit = parseLong(request.getQueryParameter("limit"));
					GlobalFsPath globalPath = GlobalFsPath.of(pubKey, fs, path);
					return node.download(globalPath, offset, limit)
							.thenApply(supplier -> HttpResponse.ok200().withBodyStream(supplier.apply(new FrameEncoder())));
				})
				.with(POST, "/" + UPLOAD, request -> {
					PubKey pubKey = PubKey.fromString(request.getQueryParameter("key"));
					String fs = request.getQueryParameter("fs");
					String path = request.getQueryParameter("path");
					long offset = parseLong(request.getQueryParameter("offset"));
					GlobalFsPath globalPath = GlobalFsPath.of(pubKey, fs, path);
					SerialSupplier<ByteBuf> body = request.getBodyStream();
					return node.upload(globalPath, offset)
							.thenCompose(consumer -> body.streamTo(consumer.apply(new FrameDecoder())))
							.thenApply($ -> HttpResponse.ok200());
				})
				.with(GET, "/" + LIST, request -> {
					PubKey pubKey = PubKey.fromString(request.getQueryParameter("key"));
					String fs = request.getQueryParameter("fs");
					String glob = request.getQueryParameter("glob");
					return node.list(GlobalFsName.of(pubKey, fs), glob)
							.thenApply(list -> {
								ByteBuf buf = ByteBufPool.allocate(sizeof(list, BinaryDataFormats::sizeof));
								BinaryDataFormats.writeCollection(buf, list, BinaryDataFormats::writeGlobalFsMetadata);
								return HttpResponse.ok200().withBody(buf);
							});
				})
				.with(DELETE, "/" + DEL, request -> {
					PubKey pubKey = PubKey.fromString(request.getQueryParameter("key"));
					String fs = request.getQueryParameter("fs");
					String glob = request.getQueryParameter("glob");
					return node.delete(GlobalFsName.of(pubKey, fs), glob)
							.thenApply($ -> HttpResponse.ok200());
				})
				.with(POST, "/" + COPY, ensureBody(request -> {
					PubKey pubKey = PubKey.fromString(request.getQueryParameter("key"));
					String fs = request.getQueryParameter("fs");
					Map<String, String> changes = BinaryDataFormats.readMap(request.getBody(), BinaryDataFormats::readString, BinaryDataFormats::readString);
					return node.copy(GlobalFsName.of(pubKey, fs), changes)
							.thenApply(set -> {
								ByteBuf buf = ByteBufPool.allocate(sizeof(set, BinaryDataFormats::sizeof));
								BinaryDataFormats.writeCollection(buf, set, BinaryDataFormats::writeString);
								return HttpResponse.ok200().withBody(buf);
							});
				}))
				.with(POST, "/" + MOVE, ensureBody(request -> {
					PubKey pubKey = PubKey.fromString(request.getQueryParameter("key"));
					String fs = request.getQueryParameter("fs");
					Map<String, String> changes = BinaryDataFormats.readMap(request.getBody(), BinaryDataFormats::readString, BinaryDataFormats::readString);
					return node.move(GlobalFsName.of(pubKey, fs), changes)
							.thenApply(set -> {
								ByteBuf buf = ByteBufPool.allocate(sizeof(set, BinaryDataFormats::sizeof));
								BinaryDataFormats.writeCollection(buf, set, BinaryDataFormats::writeString);
								return HttpResponse.ok200().withBody(buf);
							});
				}));
	}
	// endregion

	private static long parseLong(String param) throws ParseException {
		try {
			return Long.parseLong(param);
		} catch (NumberFormatException e) {
			throw new ParseException(e);
		}
	}
}
