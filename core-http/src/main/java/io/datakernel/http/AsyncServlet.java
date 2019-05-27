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

import io.datakernel.async.Promise;
import io.datakernel.exception.UncheckedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Servlet receives and responds to {@link HttpRequest} from clients across
 * HTTP. Receives {@link HttpRequest}, creates {@link HttpResponse} and sends
 * it.
 */
@FunctionalInterface
public interface AsyncServlet {
	@NotNull
	Promise<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException;

	@NotNull
	static AsyncServlet of(@NotNull Function<HttpRequest, HttpResponse> fn) {
		return request -> Promise.of(fn.apply(request));
	}

	@NotNull
	static AsyncServlet ofBlocking(@NotNull BlockingServlet blockingServlet) {
		return ofBlocking(null, blockingServlet);
	}

	@NotNull
	static AsyncServlet ofBlocking(@Nullable Executor executor, @NotNull BlockingServlet blockingServlet) {
		return request -> request.loadBody()
				.then($ -> Promise.ofBlockingCallable(executor,
						() -> blockingServlet.serve(request)));
	}
}
