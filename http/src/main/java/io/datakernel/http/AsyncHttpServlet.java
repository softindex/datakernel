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

import io.datakernel.async.CallbackRegistry;
import io.datakernel.async.ParseException;

/**
 * Servlet receives and responds to {@link HttpRequest} from clients across HTTP.
 * Receives {@link HttpRequest},  creates {@link HttpResponse} and sends it.
 */
public interface AsyncHttpServlet {
	abstract class Callback {
		public Callback() {
			CallbackRegistry.register(this);
		}

		public final void sendResult(HttpResponse httpResponse) {
			CallbackRegistry.complete(this);
			onResult(httpResponse);
		}

		public final void sendHttpError(HttpServletError httpServletError) {
			CallbackRegistry.complete(this);
			onHttpError(httpServletError);
		}

		protected abstract void onResult(HttpResponse httpResponse);

		protected abstract void onHttpError(HttpServletError httpServletError);
	}

	interface HttpErrorFormatter {
		HttpResponse formatHttpError(HttpServletError httpServletError);
	}

	/**
	 * Handles the received {@link HttpRequest},  creating the {@link HttpResponse} and responds to client with ResultCallback
	 *
	 * @param request  received request
	 * @param callback ResultCallback for handling result
	 */
	void serveAsync(HttpRequest request, Callback callback) throws ParseException;
}
