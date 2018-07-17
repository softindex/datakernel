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

import io.datakernel.async.Callback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.datakernel.http.HttpMethod.*;
import static org.junit.Assert.assertEquals;

public class MiddlewareServletTest {

	private static final String TEMPLATE = "http://www.site.org";
	private static final String DELIM = "*****************************************************************************";

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private static Callback<HttpResponse> callback(String expectedBody, int expectedCode) {
		return new Callback<HttpResponse>() {
			@Override
			public void set(HttpResponse result) {
				assertEquals(expectedBody, result.getBody() == null ? "" : result.getBody().toString());
				assertEquals(expectedCode, result.getCode());
				System.out.println(result + "  " + result.getBody());
				result.recycleBufs();
			}

			@Override
			public void setException(Throwable e) {
				assertEquals(expectedCode, ((HttpException) e).getCode());
			}
		};
	}

	@Test
	public void testBase() {
		MiddlewareServlet servlet1 = MiddlewareServlet.create();
		servlet1.with(HttpMethod.GET, "/a/b/c", new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, Callback<HttpResponse> callback) {
				callback.set(HttpResponse.ofCode(200));
			}
		});

		servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c"), callback("", 200));
		servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c/d"), callback("", 404));
		servlet1.serve(HttpRequest.post("http://some-test.com/a/b/c"), callback("", 405));

		MiddlewareServlet servlet2 = MiddlewareServlet.create();
		servlet2.with(HttpMethod.HEAD, "/a/b/c", new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, Callback<HttpResponse> callback) {
				callback.set(HttpResponse.ofCode(200));
			}
		});

		servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c"), callback("", 405));
		servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c/d"), callback("", 404));
		servlet2.serve(HttpRequest.of(HttpMethod.HEAD, "http://some-test.com/a/b/c"), callback("", 200));
	}

	@Test
	public void testProcessWildCardRequest() {
		MiddlewareServlet servlet = MiddlewareServlet.create();
		servlet.with("/a/b/c/d", new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, Callback<HttpResponse> callback) {
				callback.set(HttpResponse.ofCode(200));
			}
		});

		servlet.serve(HttpRequest.get("http://some-test.com/a/b/c/d"), callback("", 200));
		servlet.serve(HttpRequest.post("http://some-test.com/a/b/c/d"), callback("", 200));
		servlet.serve(HttpRequest.of(HttpMethod.OPTIONS, "http://some-test.com/a/b/c/d"), callback("", 200));
	}

	@Test
	public void testMicroMapping() {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/");     // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a");    // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/a/c");  // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/d");  // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/e");  // 404
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/b");    // 404
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/b/f");  // ok
		HttpRequest request8 = HttpRequest.get(TEMPLATE + "/b/g");  // ok

		AsyncServlet action = AsyncServlet.of((request, callback) -> {
			ByteBuf msg = ByteBufStrings.wrapUtf8("Executed: " + request.getPath());
			callback.set(HttpResponse.ofCode(200).withBody(msg));
			request.recycleBufs();
		});

		MiddlewareServlet a = MiddlewareServlet.create()
				.with(GET, "/c", action)
				.with(GET, "/d", action)
				.with(GET, "/", action);

		MiddlewareServlet b = MiddlewareServlet.create()
				.with(GET, "/f", action)
				.with(GET, "/g", action);

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/", action)
				.with(GET, "/a", a)
				.with(GET, "/b", b);

		System.out.println("Micro mapping" + DELIM);
		main.serve(request1, callback("Executed: /", 200));
		main.serve(request2, callback("Executed: /a", 200));
		main.serve(request3, callback("Executed: /a/c", 200));
		main.serve(request4, callback("Executed: /a/d", 200));
		main.serve(request5, callback("", 404));
		main.serve(request6, callback("", 404));
		main.serve(request7, callback("Executed: /b/f", 200));
		main.serve(request8, callback("Executed: /b/g", 200));
		System.out.println();

		request5.recycleBufs();
		request6.recycleBufs();
	}

	@Test
	public void testLongMapping() {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/");     // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a");    // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/a/c");  // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/d");  // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/e");  // 404
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/b");    // 404
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/b/f");  // ok
		HttpRequest request8 = HttpRequest.get(TEMPLATE + "/b/g");  // ok

		AsyncServlet action = AsyncServlet.of((request, callback) -> {
			ByteBuf msg = ByteBufStrings.wrapUtf8("Executed: " + request.getPath());
			callback.set(HttpResponse.ofCode(200).withBody(msg));
			request.recycleBufs();
		});

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/", action)
				.with(GET, "/a", action)
				.with(GET, "/a/c", action)
				.with(GET, "/a/d", action)
				.with(GET, "/b/f", action)
				.with(GET, "/b/g", action);

		System.out.println("Long mapping " + DELIM);
		main.serve(request1, callback("Executed: /", 200));
		main.serve(request2, callback("Executed: /a", 200));
		main.serve(request3, callback("Executed: /a/c", 200));
		main.serve(request4, callback("Executed: /a/d", 200));
		main.serve(request5, callback("", 404));
		main.serve(request6, callback("", 404));
		main.serve(request7, callback("Executed: /b/f", 200));
		main.serve(request8, callback("Executed: /b/g", 200));
		System.out.println();

		request5.recycleBufs();
		request6.recycleBufs();
	}

	@Test
	public void testOverrideHandler() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Can't map. Servlet already exists");

		MiddlewareServlet s1 = MiddlewareServlet.create()
				.with(GET, "/", new AsyncServlet() {
					@Override
					public void serve(HttpRequest request, Callback<HttpResponse> callback) {
					}
				})
				.with(GET, "/", new AsyncServlet() {
					@Override
					public void serve(HttpRequest request, Callback<HttpResponse> callback) {
					}
				});
	}

	@Test
	public void testMerge() {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/");         // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a");        // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/b");        // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/c");      // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/d");      // ok
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/a/e");      // ok
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/a/c/f");    // ok

		AsyncServlet action = AsyncServlet.of((request, callback) -> {
			ByteBuf msg = ByteBufStrings.wrapUtf8("Executed: " + request.getPath());
			callback.set(HttpResponse.ofCode(200).withBody(msg));
			request.recycleBufs();
		});

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/a", action)
				.with(GET, "/a/c", action)
				.with(GET, "/a/d", action)
				.with(GET, "/b", action)
				.with(GET, "/", MiddlewareServlet.create()
						.with(GET, "/", action)
						.with(GET, "/a/e", action)
						.with(GET, "/a/c/f", action));

		System.out.println("Merge   " + DELIM);
		main.serve(request1, callback("Executed: /", 200));
		main.serve(request2, callback("Executed: /a", 200));
		main.serve(request3, callback("Executed: /b", 200));
		main.serve(request4, callback("Executed: /a/c", 200));
		main.serve(request5, callback("Executed: /a/d", 200));
		main.serve(request6, callback("Executed: /a/e", 200));
		main.serve(request7, callback("Executed: /a/c/f", 200));
		System.out.println();
	}

	@Test
	public void testFailMerge() {
		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/c/f");    // fail

		AsyncServlet action = AsyncServlet.of((req, callback) -> {
			ByteBuf msg = ByteBufStrings.wrapUtf8("Executed: " + req.getPath());
			callback.set(HttpResponse.ofCode(200).withBody(msg));
			req.recycleBufs();
		});

		AsyncServlet anotherAction = AsyncServlet.of((req, callback) -> {
			ByteBuf msg = ByteBufStrings.wrapUtf8("Shall not be executed: " + req.getPath());
			callback.set(HttpResponse.ofCode(200).withBody(msg));
			req.recycleBufs();
		});

		MiddlewareServlet main;
		try {
			main = MiddlewareServlet.create()
					.with(GET, "/", action)
					.with(GET, "/a/e", action)
					.with(GET, "/a/c/f", action)
					.with(GET, "/", MiddlewareServlet.create()
							.with(GET, "/a/c/f", anotherAction));
		} catch (IllegalArgumentException e) {
			assertEquals("Can't map. Servlet for this method already exists", e.getMessage());
			request.recycleBufs();
			return;
		}

		main.serve(request, callback("SHALL NOT BE EXECUTED", 500));
		request.recycleBufs();
	}

	@Test
	public void testParameter() {
		AsyncServlet printParameters = AsyncServlet.of((request, callback) -> {
			String body = request.getPathParameter("id")
					+ " " + request.getPathParameter("uid")
					+ " " + request.getPathParameter("eid");
			ByteBuf bodyByteBuf = ByteBufStrings.wrapUtf8(body);
			callback.set(HttpResponse.ofCode(200).withBody(bodyByteBuf));
			request.recycleBufs();
		});

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/:id/a/:uid/b/:eid", printParameters)
				.with(GET, "/:id/a/:uid", printParameters);

		System.out.println("Parameter test " + DELIM);
		main.serve(HttpRequest.get("http://www.coursera.org/123/a/456/b/789"), callback("123 456 789", 200));
		main.serve(HttpRequest.get("http://www.coursera.org/555/a/777"), callback("555 777 null", 200));
		HttpRequest request = HttpRequest.get("http://www.coursera.org");
		main.serve(request, callback("", 404));
		System.out.println();
		request.recycleBufs();
	}

	@Test
	public void testMultiParameters() {
		AsyncServlet serveCar = AsyncServlet.of((request, callback) -> {
			ByteBuf body = ByteBufStrings.wrapUtf8("served car: " + request.getPathParameter("cid"));
			callback.set(HttpResponse.ofCode(200).withBody(body));
			request.recycleBufs();
		});

		AsyncServlet serveMan = AsyncServlet.of((request, callback) -> {
			ByteBuf body = ByteBufStrings.wrapUtf8("served man: " + request.getPathParameter("mid"));
			callback.set(HttpResponse.ofCode(200).withBody(body));
			request.recycleBufs();
		});

		MiddlewareServlet ms = MiddlewareServlet.create()
				.with(GET, "/serve/:cid/wash", serveCar)
				.with(GET, "/serve/:mid/feed", serveMan);

		System.out.println("Multi parameters " + DELIM);
		ms.serve(HttpRequest.get(TEMPLATE + "/serve/1/wash"), callback("served car: 1", 200));
		ms.serve(HttpRequest.get(TEMPLATE + "/serve/2/feed"), callback("served man: 2", 200));
		System.out.println();
	}

	@Test
	public void testDifferentMethods() {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/a/b/c/action");
		HttpRequest request2 = HttpRequest.post(TEMPLATE + "/a/b/c/action");
		HttpRequest request3 = HttpRequest.of(CONNECT, TEMPLATE + "/a/b/c/action");

		AsyncServlet post = AsyncServlet.of((request, callback) -> {
			callback.set(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("POST")));
			request.recycleBufs();
		});

		AsyncServlet get = AsyncServlet.of((request, callback) -> {
			callback.set(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("GET")));
			request.recycleBufs();
		});

		AsyncServlet wildcard = AsyncServlet.of((request, callback) -> {
			callback.set(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("WILDCARD")));
			request.recycleBufs();
		});

		MiddlewareServlet servlet = MiddlewareServlet.create()
				.with("/a/b/c/action", wildcard)
				.with(POST, "/a/b/c/action", post)
				.with(GET, "/a/b/c/action", get);

		System.out.println("Different methods " + DELIM);
		servlet.serve(request1, callback("GET", 200));
		servlet.serve(request2, callback("POST", 200));
		servlet.serve(request3, callback("WILDCARD", 200));
		System.out.println();
	}

	@Test
	public void testDefault() {
		AsyncServlet def = AsyncServlet.of((request, callback) -> {
			callback.set(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("Stopped at admin: " + request.getPartialPath())));
			request.recycleBufs();
		});

		AsyncServlet action = AsyncServlet.of((request, callback) -> {
			callback.set(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("Action executed")));
			request.recycleBufs();
		});

		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/html/admin/action");
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/html/admin/action/ban");

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/html/admin/action", action)
				.withFallback("/html/admin", def);

		System.out.println("Default stop " + DELIM);
		main.serve(request1, callback("Action executed", 200));
		main.serve(request2, callback("Stopped at admin: /action/ban", 200));
		System.out.println();
	}

	@Test
	public void test404() {
		AsyncServlet servlet = AsyncServlet.of((request, callback) -> {
			callback.set(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("All OK")));
			request.recycleBufs();
		});
		MiddlewareServlet main = MiddlewareServlet.create()
				.with("/a/:id/b/d", servlet);

		System.out.println("404 " + DELIM);
		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/123/b/c");
		main.serve(request, callback("", 404));
		System.out.println();
		request.recycleBufs();
	}

	@Test
	public void test405() {
		AsyncServlet servlet = AsyncServlet.of((request, callback) -> {
			callback.set(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("Should not execute")));
			request.recycleBufs();
		});
		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/a/:id/b/d", servlet);

		HttpRequest request = HttpRequest.post(TEMPLATE + "/a/123/b/d");
		main.serve(request, callback("", 405));
		request.recycleBufs();
	}

	@Test
	public void test405WithFallback() {
		AsyncServlet servlet = AsyncServlet.of((request, callback) -> {
			callback.set(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("Should not execute")));
			request.recycleBufs();
		});
		AsyncServlet fallback = AsyncServlet.of((request, callback) -> {
			callback.set(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("Fallback executed")));
			request.recycleBufs();
		});
		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/a/:id/b/d", servlet)
				.withFallback("/a/:id/b/d", fallback);
		main.serve(HttpRequest.post(TEMPLATE + "/a/123/b/d"), callback("Fallback executed", 200));
	}
}
