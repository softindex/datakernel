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

import io.datakernel.bytebuf.ByteBuf;

import java.util.concurrent.CompletionStage;

@SuppressWarnings({"unused", "WeakerAccess"})
public final class GzipServlet implements AsyncServlet {
	private static final int DEFAULT_BODY_SIZE_THRESHOLD = 200;

	private final AsyncServlet asyncServlet;
	private final int minBodySizeThreshold;

	public static GzipServlet create(int minBodySizeThreshold, AsyncServlet asyncServlet) {
		return new GzipServlet(minBodySizeThreshold, asyncServlet);
	}

	public static GzipServlet create(AsyncServlet asyncServlet) {
		return new GzipServlet(DEFAULT_BODY_SIZE_THRESHOLD, asyncServlet);
	}

	private GzipServlet(int minBodySizeThreshold, AsyncServlet asyncServlet) {
		this.asyncServlet = asyncServlet;
		this.minBodySizeThreshold = minBodySizeThreshold;
	}

	@Override
	public CompletionStage<HttpResponse> serve(HttpRequest request) {
		CompletionStage<HttpResponse> serve = asyncServlet.serve(request);
		if (!test(request)) return serve;

		return serve.thenApply(response ->
				test(response, minBodySizeThreshold) ? response.withBodyGzipCompression() : response);
	}

	private static boolean test(HttpRequest request) {
		return request.isAcceptEncodingGzip();
	}

	private static boolean test(HttpResponse response, int minimumSizeThreshold) {
		int code = response.getCode();
		ByteBuf body = response.getBody();
		return code == 200 && body != null && body.readRemaining() > minimumSizeThreshold;
	}
}
