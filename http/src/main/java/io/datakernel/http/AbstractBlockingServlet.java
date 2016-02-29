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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Represent an asynchronous HTTP servlet which receives and responds to requests from clients across HTTP.
 * For handling each request it uses new thread. For using this servlet you should override method doServeBlocking(),
 * in this method must be logic for creating HttpResult.
 */
public abstract class AbstractBlockingServlet extends AbstractAsyncServlet {
	private final Executor executor;
	private final EventStats requestsRejected = new EventStats();

	/**
	 * Creates a new instance of AbstractAsyncExecutorHttpServlet
	 *
	 * @param executor executor which will execute new threads
	 */
	public AbstractBlockingServlet(Eventloop eventloop, Executor executor) {
		super(eventloop);
		this.executor = executor;
	}

	protected abstract HttpResponse doServeBlocking(HttpRequest httpRequest) throws HttpServletError, ParseException;

	protected final void doServeAsync(final HttpRequest request, final Callback callback) {
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					final HttpResponse httpResponse = doServeBlocking(request);
					eventloop.execute(new Runnable() {
						@Override
						public void run() {
							callback.onResult(httpResponse);
						}
					});
				} catch (final RejectedExecutionException e) {
					requestsRejected.recordEvent();
					eventloop.execute(new Runnable() {
						@Override
						public void run() {
							callback.onHttpError(new HttpServletError(429));
						}
					});
				} catch (final HttpServletError e) {
					eventloop.execute(new Runnable() {
						@Override
						public void run() {
							callback.onHttpError(e);
						}
					});
				} catch (final ParseException e) {
					eventloop.execute(new Runnable() {
						@Override
						public void run() {
							callback.onHttpError(new HttpServletError(400, e));
						}
					});
				} catch (final Exception e) {
					eventloop.execute(new Runnable() {
						@Override
						public void run() {
							throw e;
						}
					});
				}
			}
		});
	}

	@JmxAttribute
	public EventStats getRequestsRejected() {
		return requestsRejected;
	}

}
