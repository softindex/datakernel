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

import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class StaticServlet implements AsyncHttpServlet {
	public static final Charset DEFAULT_TXT_ENCODING = StandardCharsets.UTF_8;
	public static final String DEFAULT_INDEX_FILE_NAME = "index.html"; // response for get request asking for root

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

	protected abstract void doServeAsync(String name, ForwardingResultCallback<ByteBuf> callback);

	protected HttpResponse createHttpResponse(ByteBuf buf, String path) {
		return HttpResponse.create(200)
				.body(buf)
				.contentType(getContentType(path));
	}

	@Override
	public final void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
		String path = request.getRelativePath();
		if (request.getMethod() == HttpMethod.GET && path.equals("/")) {
			path = DEFAULT_INDEX_FILE_NAME;
		} else {
			assert path.charAt(0) == '/';
			path = path.substring(1); // removing initial '/'
		}
		final ContentType type = getContentType(path);
		final String finalPath = path;
		doServeAsync(path, new ForwardingResultCallback<ByteBuf>(callback) {
			@Override
			public void onResult(ByteBuf buf) {
				callback.onResult(createHttpResponse(buf, finalPath).contentType(type));
			}
		});
	}

}