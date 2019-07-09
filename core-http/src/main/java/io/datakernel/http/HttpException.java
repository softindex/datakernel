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

import java.io.PrintWriter;
import java.io.StringWriter;

import static io.datakernel.http.ContentTypes.PLAIN_TEXT_UTF_8;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This is a special exception, that is formatted as HTTP responce with code and text from it by default.
 * It is a stackless exception.
 */
public class HttpException extends Exception {
	private final int code;

	protected HttpException(int code) {
		this.code = code;
	}

	protected HttpException(int code, String message) {
		super(message);
		this.code = code;
	}

	protected HttpException(int code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;
	}

	protected HttpException(int code, Throwable cause) {
		super(cause);
		this.code = code;
	}

	public static HttpException ofCode(int code) {
		return new HttpException(code);
	}

	public static HttpException ofCode(int code, String message) {
		return new HttpException(code, message);
	}

	public static HttpException ofCode(int code, String message, Throwable cause) {
		return new HttpException(code, message, cause);
	}

	public static HttpException ofCode(int code, Throwable cause) {
		return new HttpException(code, cause);
	}

	public static HttpException badRequest400() {
		return new HttpException(400, "Bad request");
	}

	public static HttpException notFound404() {
		return new HttpException(404, "Not found");
	}

	public static HttpException internalServerError500() {
		return new HttpException(500, "Internal server error");
	}

	public static HttpException notAllowed405() {
		return new HttpException(405, "Not allowed");
	}

	public final int getCode() {
		return code;
	}

	@Override
	public final Throwable fillInStackTrace() {
		return this;
	}

	public HttpResponse createResponse() {
		HttpResponse response = HttpResponse.ofCode(code)
				.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(PLAIN_TEXT_UTF_8));
		String message = "";
		if (getLocalizedMessage() != null) {
			message = getLocalizedMessage();
		}
		if (getCause() != null) {
			StringWriter writer = new StringWriter();
			getCause().printStackTrace(new PrintWriter(writer));
			message += "\n" + writer;
		}
		return response.withBody(message.getBytes(UTF_8));
	}

	@Override
	public String toString() {
		return "HTTP code " + code + ": " + getLocalizedMessage();
	}
}
