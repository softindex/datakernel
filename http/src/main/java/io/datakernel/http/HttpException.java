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

import io.datakernel.exception.SimpleException;

public class HttpException extends SimpleException {
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
		return new HttpException(400);
	}

	public static HttpException notFound404() {
		return new HttpException(404);
	}

	public static HttpException internalServerError500() {
		return new HttpException(500);
	}

	public static HttpException notAllowed405() {
		return new HttpException(405);
	}

	public final int getCode() {
		return code;
	}

	@Override
	public String toString() {
		return code + ": " + getMessage();
	}
}
