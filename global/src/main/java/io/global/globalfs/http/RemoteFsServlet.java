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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.global.globalsync.util.BinaryDataFormats;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.AsyncServlet.ensureBody;
import static io.datakernel.http.HttpHeaders.CONTENT_DISPOSITION;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.http.MediaTypes.OCTET_STREAM;
import static io.global.globalsync.util.BinaryDataFormats.sizeof;
import static io.global.globalsync.util.BinaryDataFormats.writeString;

public final class RemoteFsServlet {
	public static final ParseException INVALID_RANGE_FORMAT = new ParseException("Invalid range format");

	public static final String UPLOAD = "upload";
	public static final String DOWNLOAD = "download";
	public static final String LIST = "list";
	public static final String DEL = "delete";
	public static final String COPY = "copy";
	public static final String MOVE = "move";

	// region creators
	private RemoteFsServlet() {
		throw new AssertionError("nope.");
	}

	public static AsyncServlet wrap(FsClient client) {
		return MiddlewareServlet.create()
				.with(GET, "/" + DOWNLOAD, request -> {
					String path = request.getQueryParameter("path");
					long[] range = getRange(request.getQueryParameterOrNull("range"));
					long offset = range[0];
					long limit = range[1];
					String name = path;
					int lastSlash = path.lastIndexOf('/');
					if (lastSlash != -1) {
						name = path.substring(lastSlash + 1);
					}
					return client.download(path, offset, limit)
							.thenApply(HttpResponse.ok200()
									.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(OCTET_STREAM)))
									.withHeader(CONTENT_DISPOSITION, HttpHeaderValue.of("attachment; filename=\"" + name + "\""))
									::withBodyStream);
				})
				.with(POST, "/" + UPLOAD, request -> {
					String path = request.getQueryParameter("path");
					long offset = parseLong(request.getQueryParameter("offset"));
					return client.upload(path, offset)
							.thenCompose(request.getBodyStream()::streamTo)
							.thenApply($ -> HttpResponse.ok200());
				})
				.with(GET, "/" + LIST, request -> {
					String glob = request.getQueryParameter("glob");
					return client.list(glob)
							.thenApply(list -> {
								ByteBuf buf = ByteBufPool.allocate(sizeof(list, BinaryDataFormats::sizeof));
								BinaryDataFormats.writeCollection(buf, list, BinaryDataFormats::writeFileMetadata);
								return HttpResponse.ok200().withBody(buf);
							});
				})
				.with(DELETE, "/" + DEL, request -> client.delete(request.getQueryParameter("glob")).thenApply($ -> HttpResponse.ok200()))
				.with(POST, "/" + COPY, ensureBody(request -> {
					Map<String, String> changes = BinaryDataFormats.readMap(request.getBody(), BinaryDataFormats::readString, BinaryDataFormats::readString);
					return client.copy(changes)
							.thenApply(set -> {
								ByteBuf buf = ByteBufPool.allocate(sizeof(set, BinaryDataFormats::sizeof));
								set.forEach(s -> writeString(buf, s));
								return HttpResponse.ok200().withBody(buf);
							});
				}))
				.with(POST, "/" + MOVE, ensureBody(request -> {
					Map<String, String> changes = BinaryDataFormats.readMap(request.getBody(), BinaryDataFormats::readString, BinaryDataFormats::readString);
					return client.move(changes)
							.thenApply(set -> {
								ByteBuf buf = ByteBufPool.allocate(sizeof(set, BinaryDataFormats::sizeof));
								set.forEach(s -> writeString(buf, s));
								return HttpResponse.ok200().withBody(buf);
							});
				}));
	}
	// endregion

	private static long[] getRange(@Nullable String param) throws ParseException {
		long[] range = {0, -1};
		if (param == null) {
			return range;
		}
		int index = param.indexOf(':');
		if (index == -1) {
			try {
				range[0] = Long.parseUnsignedLong(param);
			} catch (NumberFormatException e) {
				throw INVALID_RANGE_FORMAT;
			}
			return range;
		}
		String[] parts = param.split(":");
		if (parts.length != 2) {
			throw INVALID_RANGE_FORMAT;
		}
		try {
			if (!parts[0].equals("")) {
				range[0] = Long.parseUnsignedLong(parts[0]);
			}
			range[1] = Long.parseUnsignedLong(parts[1]);
		} catch (NumberFormatException e) {
			throw INVALID_RANGE_FORMAT;
		}
		return range;
	}

	private static long parseLong(String param) throws ParseException {
		try {
			return Long.parseLong(param);
		} catch (NumberFormatException e) {
			throw new ParseException(e);
		}
	}

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newSingleThreadExecutor();
		FsClient client = LocalFsClient.create(eventloop, executor, Paths.get("/tmp/TESTS"));

		AsyncHttpServer server = AsyncHttpServer.create(eventloop, wrap(client))
				.withListenPort(8080);
		server.listen();

		eventloop.run();
	}
}
