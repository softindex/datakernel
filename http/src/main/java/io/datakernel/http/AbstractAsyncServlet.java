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

import io.datakernel.async.ParseException;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represent an asynchronous HTTP servlet which receives and responds to requests from clients across HTTP.
 * For using this servlet you should override method doServeAsync(, in this method must be logic
 * for handling requests and creating result.
 */
public abstract class AbstractAsyncServlet implements AsyncHttpServlet, EventloopJmxMBean {
	protected static final Logger logger = LoggerFactory.getLogger(AbstractAsyncServlet.class);

	protected final Eventloop eventloop;

	// JMX
	private final EventStats requests = new EventStats();
	private final ExceptionStats errors = new ExceptionStats();
	private final ValueStats requestsTimings = new ValueStats();
	private final ValueStats errorsTimings = new ValueStats();

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
		doServeAsync(request, new Callback() {
			@Override
			public void onResult(HttpResponse result) {
				int duration = (int) (eventloop.currentTimeMillis() - timestamp);
				requestsTimings.recordValue(duration);
				callback.onResult(result);
			}

			@Override
			public void onHttpError(HttpServletError httpServletError) {
				int duration = (int) (eventloop.currentTimeMillis() - timestamp);
				errorsTimings.recordValue(duration);
				errors.recordException(httpServletError, extractUrl(request));
				callback.onHttpError(httpServletError);
			}
		});
	}

	// jmx
	protected boolean isMonitoring(HttpRequest request) {
		return true;
	}

	private static String extractUrl(HttpRequest request) {
		return "url: " + request.toString();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public final EventStats getRequests() {
		return requests;
	}

	@JmxAttribute
	public final ExceptionStats getErrors() {
		return errors;
	}

	@JmxAttribute
	public final ValueStats getRequestsTimings() {
		return requestsTimings;
	}

	@JmxAttribute
	public final ValueStats getErrorsTimings() {
		return errorsTimings;
	}

}
