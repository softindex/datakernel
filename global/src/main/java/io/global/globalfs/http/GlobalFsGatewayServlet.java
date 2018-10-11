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

package io.global.globalfs.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.*;
import io.datakernel.serial.SerialSupplier;
import io.global.globalfs.api.GlobalFsGateway;
import io.global.globalfs.api.GlobalFsPath;

import static io.datakernel.http.HttpHeaders.CONTENT_DISPOSITION;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.PUT;
import static io.datakernel.http.MediaTypes.OCTET_STREAM;
import static io.global.globalfs.util.HttpDataFormats.*;

public final class GlobalFsGatewayServlet {
	public static final String UPLOAD = "upload";
	public static final String DOWNLOAD = "download";

	private GlobalFsGatewayServlet() {
		throw new AssertionError("nope.");
	}

	public static AsyncServlet wrap(GlobalFsGateway gateway) {
		return MiddlewareServlet.create()
				.with(GET, "/" + DOWNLOAD + "/:key/:fs",
						MiddlewareServlet.create().withFallback(request -> {
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
						}))
				.with(PUT, "/" + UPLOAD + "/:key/:fs",
						MiddlewareServlet.create().withFallback(request -> {
							SerialSupplier<ByteBuf> body = request.getBodyStream();
							return gateway.upload(parsePath(request), parseOffset(request))
									.thenCompose(body::streamTo)
									.thenApply($ -> HttpResponse.ok200());
						}));
	}
}
