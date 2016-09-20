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

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Represent an asynchronous HTTP servlet which receives and responds to requests from clients across HTTP.
 * For using this servlet you should override method doServeAsync(, in this method must be logic
 * for handling requests and creating result.
 */
public abstract class AbstractAsyncServlet implements AsyncHttpServlet, EventloopJmxMBean {
	protected static final Logger logger = LoggerFactory.getLogger(AbstractAsyncServlet.class);

	protected final Eventloop eventloop;

	// jmx
	private final EventStats requests = EventStats.create();
	private final ExceptionStats errors = ExceptionStats.create();
	private final ValueStats requestsTimings = ValueStats.create();
	private final ValueStats errorsTimings = ValueStats.create();
	private final Map<Integer, ExceptionStats> errorCodeToStats = new HashMap<>();

	protected AbstractAsyncServlet(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	/**
	 * Method with logic for handling request and creating the response to client
	 *
	 * @param request  received request from client
	 * @param callback callback for handling result
	 */
	protected abstract void doServeAsync(HttpRequest request, Callback callback) throws ParseException;

	/**
	 * Handles the received {@link HttpRequest},  creates the {@link HttpResponse} and responds to client with
	 * {@link ResultCallback}
	 *
	 * @param request  received request
	 * @param callback ResultCallback for handling result
	 */
	@Override
	public final void serveAsync(final HttpRequest request, final Callback callback) throws ParseException {
		if (!isMonitoring(request)) {
			doServeAsync(request, callback);
			return;
		}
		requests.recordEvent();
		final long timestamp = eventloop.currentTimeMillis();
		try {
			doServeAsync(request, new Callback() {
				@Override
				public void onResult(HttpResponse result) {
					// jmx
					int duration = (int) (eventloop.currentTimeMillis() - timestamp);
					requestsTimings.recordValue(duration);

					callback.onResult(result);
				}

				@Override
				public void onHttpError(HttpServletError httpServletError) {
					// jmx
					int duration = (int) (eventloop.currentTimeMillis() - timestamp);
					errorsTimings.recordValue(duration);
					recordError(httpServletError, request);

					callback.onHttpError(httpServletError);
				}
			});
		} catch (ParseException parseException) {
			int badRequestHttpCode = 400;
			HttpServletError error = new HttpServletError(badRequestHttpCode, parseException);

			// jmx
			int duration = (int) (eventloop.currentTimeMillis() - timestamp);
			errorsTimings.recordValue(duration);
			recordError(error, request);

			callback.onHttpError(error);
		}
	}

	private void recordError(HttpServletError error, HttpRequest request) {
		String url = extractUrl(request);
		Throwable cause = error.getCause();
		errors.recordException(cause, url);
		ExceptionStats stats = ensureStats(error.getCode());
		stats.recordException(cause, url);
	}

	private ExceptionStats ensureStats(int code) {
		ExceptionStats stats = errorCodeToStats.get(code);
		if (stats == null) {
			stats = ExceptionStats.create();
			errorCodeToStats.put(code, stats);
		}
		return stats;
	}

	// jmx
	protected boolean isMonitoring(HttpRequest request) {
		return true;
	}

	private static String extractUrl(HttpRequest request) {
		return "url: " + request.getFullUrl();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public final EventStats getRequests() {
		return requests;
	}

	@JmxAttribute(description = "requests that were handled with error")
	public final ExceptionStats getErrors() {
		return errors;
	}

	@JmxAttribute(description = "duration of handling one request in case of success")
	public final ValueStats getRequestsTimings() {
		return requestsTimings;
	}

	@JmxAttribute(description = "duration of handling one request in case of error")
	public final ValueStats getErrorsTimings() {
		return errorsTimings;
	}

	@JmxAttribute(description = "servlet errors distributed by http code")
	public final Map<Integer, ExceptionStats> getErrorCodeToStats() {
		return errorCodeToStats;
	}
}
