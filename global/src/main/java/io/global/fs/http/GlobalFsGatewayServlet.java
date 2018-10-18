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
import io.datakernel.http.*;
import io.datakernel.serial.SerialSupplier;
import io.global.fs.api.GlobalFsGateway;
import io.global.fs.api.GlobalFsPath;
import io.global.ot.util.BinaryDataFormats;

import static io.datakernel.http.HttpHeaders.CONTENT_DISPOSITION;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.http.MediaTypes.OCTET_STREAM;
import static io.global.fs.util.HttpDataFormats.*;
import static io.global.ot.util.BinaryDataFormats.sizeof;

public final class GlobalFsGatewayServlet implements AsyncServlet {
	public static final String DOWNLOAD = "download";
	public static final String UPLOAD = "upload";
	public static final String LIST = "list";
	public static final String DEL = "delete";

	private final AsyncServlet servlet;

	@Inject
	public GlobalFsGatewayServlet(GlobalFsGateway gateway) {
		this.servlet = MiddlewareServlet.create()
				.with(GET, "/" + DOWNLOAD + "/:key/:fs/:path*", request -> {
					long[] range = parseRange(request);
					GlobalFsPath path = parsePath(request);
					String name = path.getPath();
					int lastSlash = name.lastIndexOf('/');
					if (lastSlash != -1) {
						name = name.substring(lastSlash + 1);
					}
					return gateway.download(path, range[0], range[1])
							.thenApply(HttpResponse.ok200()
									.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(OCTET_STREAM)))
									.withHeader(CONTENT_DISPOSITION, HttpHeaderValue.of("attachment; filename=\"" + name + "\""))
									::withBodyStream);
				})
				.with(PUT, "/" + UPLOAD + "/:key/:fs/:path*", request ->
						gateway.upload(parsePath(request), parseOffset(request))
								.thenCompose(request.getBodyStream()::streamTo)
								.thenApply($ -> HttpResponse.ok200()))
				.with(GET, "/" + LIST + "/:key/:fs", request ->
						gateway.list(parseSpace(request), request.getQueryParameter("glob"))
								.thenApply(list -> HttpResponse.ok200()
										.withBodyStream(SerialSupplier.ofStream(list.stream()
												.map(meta -> {
													byte[] bytes = meta.toBytes();
													ByteBuf buf = ByteBufPool.allocate(sizeof(bytes));
													BinaryDataFormats.writeBytes(buf, bytes);
													return buf;
												})))))
				.with(DELETE, "/" + DEL + "/:key/:fs", request ->
						gateway.delete(parseSpace(request), request.getQueryParameter("glob"))
								.thenApply($ -> HttpResponse.ok200()));
	}

	@Override
	public Stage<HttpResponse> serve(HttpRequest request) throws ParseException, UncheckedException {
		return servlet.serve(request);
	}
}
