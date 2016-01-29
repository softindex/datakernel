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
import io.datakernel.jmx.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;

/**
 * Represent an asynchronous HTTP servlet which receives and responds to requests from clients across HTTP.
 * For using this servlet you should override method doServeAsync(, in this method must be logic
 * for handling requests and creating result.
 */
@JmxMBean
public abstract class AbstractAsyncServlet implements AsyncHttpServlet {
	protected static final Logger logger = LoggerFactory.getLogger(AbstractAsyncServlet.class);
	public static final RejectedExecutionException THROTTLED_EXCEPTION = new RejectedExecutionException("Throttled");

	protected final Eventloop eventloop;

	// JMX
	private ValueStats timings = new ValueStats();
	private EventStats requests = new EventStats();
	private EventStats requestsThrottled = new EventStats();
	private final ExceptionStats exceptions = new ExceptionStats();

	private static class RequestExceptionDetails {
		private final String requestUrl;
		//		private final ByteBuf requestBody; TODO (vmykhalko)

		public RequestExceptionDetails(HttpRequest request) {
			this.requestUrl = request.toString();
		}

		@Override
		public String toString() {
			return "RequestExceptionDetails{" +
					"requestUrl='" + requestUrl + '\'' +
					'}';
		}
	}

	protected AbstractAsyncServlet(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	protected boolean isRequestThrottled(HttpRequest request) {
		if (eventloop.throttlingController != null)
			return eventloop.throttlingController.isRequestThrottled();
		return false;
	}

	/**
	 * Handles the received {@link HttpRequest},  creates the {@link HttpResponse} and responds to client with
	 * {@link ResultCallback}
	 *
	 * @param request  received request
	 * @param callback ResultCallback for handling result
	 */
	@Override
	public final void serveAsync(final HttpRequest request, final ResultCallback<HttpResponse> callback) {
		requests.recordEvent();
		if (isRequestThrottled(request)) {
			requestsThrottled.recordEvent();
			handleRejectedRequest(request, THROTTLED_EXCEPTION, callback);
			return;
		}
		final long timestamp = eventloop.currentTimeMillis();
		doServeAsync(request, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				int duration = (int) (eventloop.currentTimeMillis() - timestamp);
				timings.recordValue(duration);
				callback.onResult(result);
			}

			@Override
			public void onException(Exception exception) {
				handleException(request, exception, callback);
			}
		});
	}

	/**
	 * Method with logic for handling request and creating the response to client
	 *
	 * @param request  received request from client
	 * @param callback callback for handling result
	 */
	protected abstract void doServeAsync(HttpRequest request, ResultCallback<HttpResponse> callback);

	protected final void handleException(HttpRequest request, Exception e, ResultCallback<HttpResponse> callback) {
		HttpResponse response;
		if (logger.isErrorEnabled()) {
			logger.error("Exception on {}: {}", request, e.toString());
		}
		exceptions.recordException(e, new RequestExceptionDetails(request), eventloop.currentTimeMillis());
		response = formatErrorResponse(request, e);
		if (response != null) {
			callback.onResult(response);
		} else {
			callback.onException(e);
		}
	}

	protected final void handleRejectedRequest(HttpRequest request, RejectedExecutionException e, ResultCallback<HttpResponse> callback) {
		if (logger.isWarnEnabled()) {
			logger.warn("Request rejected {} : {}", request, e.toString());
		}
		HttpResponse response = formatRejectedResponse(request, e);
		if (response != null)
			callback.onResult(response);
		else
			callback.onException(e);
	}

	protected HttpResponse formatErrorResponse(HttpRequest request, Exception e) {
		return null;
	}

	protected HttpResponse formatRejectedResponse(HttpRequest request, RejectedExecutionException e) {
		return formatErrorResponse(request, e);
	}

	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public final EventStats getRequests() {
		return requests;
	}

	@JmxAttribute
	public final EventStats getRequestsThrottled() {
		return requestsThrottled;
	}

	@JmxAttribute
	public final ExceptionStats getExceptions() {
		return exceptions;
	}

	@JmxAttribute
	public final ValueStats getTimings() {
		return timings;
	}
}
