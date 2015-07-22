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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.http.middleware.*;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static org.junit.Assert.assertEquals;

public class MiddlewareServletTest {

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void test() throws Exception {
		MiddlewareServlet servlet = new MiddlewareServlet();

		servlet.use(new HttpSuccessHandler() {
			@Override
			public void handle(final HttpRequest request, final MiddlewareRequestContext context) {
				System.out.println("1");
				context.setAttachment("key", "value");
				context.next(request);
			}
		});

		servlet.get("/", new HttpSuccessHandler() {
			@Override
			public void handle(final HttpRequest request, final MiddlewareRequestContext context) {
				Executors.newCachedThreadPool().submit(new Runnable() {
					@Override
					public void run() {
						System.out.println("2");
						context.next(request);
					}
				});
			}
		});

		servlet.get("/a", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("3");
				context.next(request);
			}
		});

		servlet.get("/a/b/", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("4");
				context.next(request);
			}
		});

		servlet.use("/A", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("5");
				context.next(request);
			}
		});

		servlet.get("/a/b/c", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("6");
				context.next(request);
			}
		});

		servlet.post("/", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("7");
				context.next(request);
			}
		});

		servlet.use("/", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("8");
				context.next(new Exception(), request);
			}
		});

		servlet.use("/", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("9");
				context.next(request);
			}
		});

		servlet.use(new HttpErrorHandler() {
			@Override
			public void handle(Exception exception, HttpRequest request, MiddlewareRequestErrorContext context) {
				System.out.println("error-1");
				context.next(exception, request);
			}
		});

		servlet.use(new HttpErrorHandler() {
			@Override
			public void handle(Exception exception, HttpRequest request, MiddlewareRequestErrorContext context) {
				System.out.println("error-2");
				context.send(HttpResponse.internalServerError500());
			}
		});

		servlet.use(new HttpErrorHandler() {
			@Override
			public void handle(Exception exception, HttpRequest request, MiddlewareRequestErrorContext context) {
				System.out.println("error-3");
				context.next(exception, request);
			}
		});

		String firstRequestUrl = "http://localhost:5588/";
		String secondRequestUrl = "http://localhost:5588/a/b";

		System.out.println("Executing request to " + firstRequestUrl);
		servlet.serveAsync(HttpRequest.get(firstRequestUrl), new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				System.out.println(result);
			}

			@Override
			public void onException(Exception exception) {
				exception.printStackTrace();
			}
		});

		Thread.sleep(250);

		System.out.println("Executing request to " + secondRequestUrl);
		servlet.serveAsync(HttpRequest.get(secondRequestUrl), new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				System.out.println(result);
			}

			@Override
			public void onException(Exception exception) {
				exception.printStackTrace();
			}
		});

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testWithRuntimeExceptions() throws Exception {
		MiddlewareServlet servlet = new MiddlewareServlet();

		servlet.use(new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("1");
				context.next(request);
			}
		});

		servlet.get("/", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("2");
				throw new RuntimeException("Runtime exception in success handler.");
			}
		});

		servlet.get("/abc", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("3");
				context.next(request);
			}
		});

		servlet.post("/", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("4");
				context.next(request);
			}
		});

		servlet.use("/", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("5");
				context.next(new Exception(), request);
			}
		});

		servlet.use("/", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.println("6");
				context.next(request);
			}
		});

		servlet.use(new HttpErrorHandler() {
			@Override
			public void handle(Exception exception, HttpRequest request, MiddlewareRequestErrorContext context) {
				System.out.println(exception.getMessage());
				context.next(exception, request);
			}
		});

		servlet.use(new HttpErrorHandler() {
			@Override
			public void handle(Exception exception, HttpRequest request, MiddlewareRequestErrorContext context) {
				System.out.println(exception.getMessage());
				throw new RuntimeException("Exception in error handler.");
			}
		});

		servlet.use(new HttpErrorHandler() {
			@Override
			public void handle(Exception exception, HttpRequest request, MiddlewareRequestErrorContext context) {
				System.out.println(exception.getMessage());
				context.next(exception, request);
			}
		});

		servlet.serveAsync(HttpRequest.get("http://localhost:5588/"), new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				System.out.println(result);
			}

			@Override
			public void onException(Exception exception) {
				exception.printStackTrace();
			}
		});

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testUrlParameters() throws Exception {
		MiddlewareServlet servlet = new MiddlewareServlet();

		servlet.use("/user/:id/:name", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.format("id: %s%n", context.getUrlParameter("id"));
				System.out.format("name: %s%n", context.getUrlParameter("name"));
			}
		});

		String requestUrl = "http://localhost:5588/user/123/joe";

		System.out.println("Executing request to " + requestUrl);

		servlet.serveAsync(HttpRequest.get(requestUrl), new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				System.out.println(result);
			}

			@Override
			public void onException(Exception exception) {
				exception.printStackTrace();
			}
		});

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testOptionalUrlParameters() throws Exception {
		MiddlewareServlet servlet = new MiddlewareServlet();

		servlet.use("/user/:id/:age/:name?", new HttpSuccessHandler() {
			@Override
			public void handle(HttpRequest request, MiddlewareRequestContext context) {
				System.out.format("id: %s%n", context.getUrlParameter("id"));
				System.out.format("age: %s%n", context.getUrlParameter("age"));
				System.out.format("name: %s%n", context.getUrlParameter("name"));
			}
		});

		String requestUrl = "http://localhost:5588/user/123/21/";

		System.out.println("Executing request to " + requestUrl);

		servlet.serveAsync(HttpRequest.get(requestUrl), new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				System.out.println(result);
			}

			@Override
			public void onException(Exception exception) {
				exception.printStackTrace();
			}
		});

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}