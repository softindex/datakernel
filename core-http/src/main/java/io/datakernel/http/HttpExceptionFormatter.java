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

package io.datakernel.http;

import org.jetbrains.annotations.NotNull;

import java.io.PrintWriter;
import java.io.StringWriter;

import static io.datakernel.http.ContentTypes.PLAIN_TEXT_UTF_8;
import static io.datakernel.http.HttpHeaders.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This is an interface for the formatter function for HTTP.
 * It converts unhandled checked exceptions that could be returned
 * from servers root servlet and transforms them into HTTP error responses.
 */
@FunctionalInterface
public interface HttpExceptionFormatter {
	@NotNull
	HttpResponse formatException(@NotNull Throwable e);

	HttpExceptionFormatter DEFAULT_FORMATTER = e -> {
		HttpResponse response;
		if (e instanceof HttpException) {
			response = HttpResponse.ofCode(((HttpException) e).getCode())
					.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(PLAIN_TEXT_UTF_8));
			if (e.getLocalizedMessage() != null) {
				response.withBody(e.getLocalizedMessage().getBytes(UTF_8));
			}
		} else {
			// default formatter leaks no information about unknown exceptions
			response = HttpResponse.ofCode(500)
					.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(PLAIN_TEXT_UTF_8));
		}
		return response
				.withHeader(CACHE_CONTROL, "no-store")
				.withHeader(PRAGMA, "no-cache")
				.withHeader(AGE, "0");
	};

	/**
	 * This formatter prints the stacktrace of the exception into the HTTP response.
	 */
	HttpExceptionFormatter DEBUG_FORMATTER = e -> {
		HttpResponse response;
		if (e instanceof HttpException) {
			response = HttpResponse.ofCode(((HttpException) e).getCode())
					.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(PLAIN_TEXT_UTF_8));
			if (e.getLocalizedMessage() != null) {
				response.withPlainText(e.getLocalizedMessage());
			}
		} else {
			StringWriter writer = new StringWriter();
			e.printStackTrace(new PrintWriter(writer));
			response = HttpResponse.ofCode(500)
					.withPlainText(writer.toString());
		}
		return response
				.withHeader(CACHE_CONTROL, "no-store")
				.withHeader(PRAGMA, "no-cache")
				.withHeader(AGE, "0");
	};
}
