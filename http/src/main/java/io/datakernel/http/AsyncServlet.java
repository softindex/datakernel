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

import io.datakernel.async.AsyncFunction;
import io.datakernel.async.Callback;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.datakernel.async.Callback.stageToCallback;

/**
 * Servlet receives and responds to {@link HttpRequest} from clients across
 * HTTP. Receives {@link HttpRequest}, creates {@link HttpResponse} and sends
 * it.
 */
public interface AsyncServlet {
	default Stage<HttpResponse> serve(HttpRequest request) {
		SettableStage<HttpResponse> result = new SettableStage<>();
		serve(request, result);
		return result;
	}

	default void serve(HttpRequest request, Callback<HttpResponse> callback) {
		stageToCallback(serve(request), callback);
	}

	static AsyncServlet of(AsyncFunction<HttpRequest, HttpResponse> function) {
		return new AsyncServlet() {
			@Override
			public Stage<HttpResponse> serve(HttpRequest request) {
				return function.apply(request);
			}
		};
	}

	static AsyncServlet of(BiConsumer<HttpRequest, Callback<HttpResponse>> consumer) {
		return new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, Callback<HttpResponse> callback) {
				consumer.accept(request, callback);
			}
		};
	}

	static AsyncServlet ofBlocking(Function<HttpRequest, HttpResponse> function) {
		return new AsyncServlet() {
			@Override
			public Stage<HttpResponse> serve(HttpRequest request) {
				return Stage.of(function.apply(request));
			}
		};
	}
}
