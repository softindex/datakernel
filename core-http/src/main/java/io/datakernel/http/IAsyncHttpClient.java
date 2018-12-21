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

import io.datakernel.async.Promise;

import java.util.function.Function;

public interface IAsyncHttpClient {
	Promise<HttpResponse> request(HttpRequest request);

	static Function<HttpResponse, Promise<HttpResponse>> ensureOk200() {
		return response -> {
			if (response.getCode() == 200) {
				return Promise.of(response);
			}
			return Promise.ofException(HttpException.ofCode(response.getCode()));
		};
	}

	static Function<HttpResponse, Promise<HttpResponse>> ensureStatusCode(int... codes) {
		return response -> {
			for (int code : codes) {
				if (response.getCode() == code) {
					return Promise.of(response);
				}
			}
			return Promise.ofException(HttpException.ofCode(response.getCode()));
		};
	}

}
