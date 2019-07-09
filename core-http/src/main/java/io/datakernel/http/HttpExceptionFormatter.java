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

import static io.datakernel.http.HttpHeaders.*;

/**
 * This is an interface for the formatter function for HTTP.
 * It converts unhandled checked exceptions that could be returned
 * from servers root servlet and transforms them into HTTP error responses.
 */
@FunctionalInterface
public interface HttpExceptionFormatter {
	String INTERNAL_SERVER_ERROR_HTML =
			"<!doctype html>" +
					"<html lang=\"en\"><head><meta charset=\"UTF-8\"><title>Internal Server Error</title></head>" +
					"<body>" +
					"<h1 style=\"text-align: center;\">Internal Server Error</h1>" +
					"<hr><p style=\"text-align: center;\">DataKernel 3.0.0</p>" +
					"</body>" +
					"</html>";

	@NotNull
	HttpResponse formatException(@NotNull Throwable e);

	/**
	 * Standard formatter maps all exceptions except HttpException to an empty response with 500 status code.
	 * HttpExceptions are mapped to a response with their status code, message and stacktrace of the cause if it was specified.
	 */
	HttpExceptionFormatter DEFAULT_FORMATTER = e -> {
		HttpResponse response;
		if (e instanceof HttpException) {
			response = ((HttpException) e).createResponse();
		} else {
			// default formatter leaks no information about unknown exceptions
			response = HttpResponse.ofCode(500)
					.withHtml(INTERNAL_SERVER_ERROR_HTML);
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
			response = ((HttpException) e).createResponse();
		} else {
			response = DebugStacktraceRenderer.render(e);
		}
		return response
				.withHeader(CACHE_CONTROL, "no-store")
				.withHeader(PRAGMA, "no-cache")
				.withHeader(AGE, "0");
	};

	/**
	 * This formatter if either one of {@link #DEFAULT_FORMATTER} or {@link #DEBUG_FORMATTER}, depending on whether
	 * the application was started from the IntelliJ IDE or not.
	 */
	HttpExceptionFormatter COMMON_FORMATTER =
			System.getProperty("java.class.path", "").contains("idea_rt.jar") ?
					DEBUG_FORMATTER :
					DEFAULT_FORMATTER;
}
