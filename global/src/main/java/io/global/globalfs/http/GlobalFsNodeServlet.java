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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.global.common.PubKey;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsNode;
import io.global.globalfs.api.GlobalFsPath;
import io.global.globalfs.transformers.FrameDecoder;
import io.global.globalfs.transformers.FrameEncoder;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;

public final class GlobalFsNodeServlet {
	public static final String UPLOAD = "/upload";
	public static final String DOWNLOAD = "/download";
	public static final String LIST = "/list";
	public static final String COPY = "/copy";
	public static final String MOVE = "/move";

	//	public static final String SETTINGS = "/settings"; //TODO ?

	private static int parseUnsignedInt(@Nullable String param) {
		try {
			//noinspection ConstantConditions parseUnsignedInt DOES accept nulls (and throws an NFE)
			return Integer.parseUnsignedInt(param);
		} catch (NumberFormatException e) {
			return -1;
		}
	}

	public static AsyncServlet wrap(GlobalFsNode node) {
		return MiddlewareServlet.create()
				.with(POST, UPLOAD, request -> {
					PubKey pubKey = GlobalFsName.deserializePubKey(request.getQueryParameter("key"));
					String filesystem = request.getQueryParameter("filesystem");
					String path = request.getQueryParameter("path");
					int offset = parseUnsignedInt(request.getQueryParameter("offset"));
					if (pubKey == null || filesystem == null || path == null || offset == -1) {
						return Stage.ofException(HttpException.badRequest400());
					}
					GlobalFsPath globalPath = GlobalFsPath.of(pubKey, filesystem, path);
					return node.upload(globalPath, offset)
							.thenCompose(consumer -> request.getBodyStream().streamTo(consumer.apply(new FrameDecoder())))
							.thenApply($ -> HttpResponse.ok200());
				})
				.with(GET, DOWNLOAD, request -> {
					PubKey pubKey = GlobalFsName.deserializePubKey(request.getQueryParameter("key"));
					String filesystem = request.getQueryParameter("filesystem");
					String path = request.getQueryParameter("path");
					int offset = parseUnsignedInt(request.getQueryParameter("offset"));
					int limit = parseUnsignedInt(request.getQueryParameter("limit"));
					if (pubKey == null || filesystem == null || path == null || offset == -1 || limit == -1) {
						return Stage.ofException(HttpException.badRequest400());
					}
					GlobalFsPath globalPath = GlobalFsPath.of(pubKey, filesystem, path);
					return node.download(globalPath, offset, limit)
							.thenApply(supplier -> HttpResponse.ok200().withBody(supplier.apply(new FrameEncoder())));
				});
	}
}
