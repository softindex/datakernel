/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.http;

import io.datakernel.async.SettableStage;
import io.datakernel.bytebuf.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

@SuppressWarnings("ThrowableInstanceNeverThrown, WeakerAccess")
public abstract class StaticServlet implements AsyncServlet {
	public static final Charset DEFAULT_TXT_ENCODING = StandardCharsets.UTF_8;
	public static final String DEFAULT_INDEX_FILE_NAME = "index.html"; // response for get request asking for root
	public static final HttpException BAD_PATH_ERROR = HttpException.ofCode(400, "Bad path and query section");
	public static final HttpException METHOD_NOT_ALLOWED = HttpException.ofCode(405, "Only GET is being allowed");

	protected StaticServlet() {
	}

	protected ContentType getContentType(String path) {
		int pos = path.lastIndexOf(".");
		if (pos != -1) {
			path = path.substring(pos + 1);
		}
		MediaType mime = MediaTypes.getByExtension(path);
		if (mime == null) {
			mime = MediaTypes.OCTET_STREAM;
		}
		ContentType type;
		if (mime.isTextType()) {
			type = ContentType.of(mime, DEFAULT_TXT_ENCODING);
		} else {
			type = ContentType.of(mime);
		}
		return type;
	}

	protected abstract CompletionStage<ByteBuf> doServeAsync(String name);

	protected HttpResponse createHttpResponse(ByteBuf buf, String path) {
		return HttpResponse.ofCode(200)
				.withBody(buf)
				.withContentType(getContentType(path));
	}

	@Override
	public final CompletionStage<HttpResponse> serve(final HttpRequest request) {
		String path = request.getRelativePath();

		if (request.getMethod() != HttpMethod.GET) return SettableStage.immediateFailedStage(METHOD_NOT_ALLOWED);
		if (path.isEmpty() || path.charAt(0) != '/') return SettableStage.immediateFailedStage(BAD_PATH_ERROR);

		if (path.equals("/")) {
			path = DEFAULT_INDEX_FILE_NAME;
		} else {
			path = path.substring(1); // removing initial '/'
		}
		final String finalPath = path;

		return doServeAsync(path).thenApply(byteBuf -> createHttpResponse(byteBuf, finalPath));
	}

}