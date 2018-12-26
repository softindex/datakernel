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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.HttpMethod.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class MiddlewareServletTest {
	private static final String TEMPLATE = "http://www.site.org";
	private static final String DELIM = "*****************************************************************************";

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	void check(Promise<HttpResponse> promise, String expectedBody, int expectedCode) {
		assertTrue(promise.isComplete());
		if (promise.isResult()) {
			HttpResponse result = promise.materialize().getResult();
			assertEquals(expectedBody, result.getBody().materialize().getResult().asString(UTF_8));
			assertEquals(expectedCode, result.getCode());
			result.recycle();
		} else {
			assertEquals(expectedCode, ((HttpException) promise.materialize().getException()).getCode());
		}
	}

	@Test
	public void testBase() {
		MiddlewareServlet servlet1 = MiddlewareServlet.create();

		AsyncServlet subservlet = request -> Promise.of(HttpResponse.ofCode(200).withBody("".getBytes(UTF_8)));

		servlet1.with(HttpMethod.GET, "/a/b/c", subservlet);

		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c")), "", 200);
		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c")), "", 200);
		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c/d")), "", 404);
		check(servlet1.serve(HttpRequest.post("http://some-test.com/a/b/c")), "", 404);

		MiddlewareServlet servlet2 = MiddlewareServlet.create();
		servlet2.with(HttpMethod.HEAD, "/a/b/c", subservlet);

		check(servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c")), "", 404);
		check(servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c/d")), "", 404);
		check(servlet2.serve(HttpRequest.of(HttpMethod.HEAD, "http://some-test.com/a/b/c")), "", 200);
	}

	@Test
	public void testProcessWildCardRequest() {
		MiddlewareServlet servlet = MiddlewareServlet.create();
		servlet.with("/a/b/c/d", request -> Promise.of(HttpResponse.ofCode(200).withBody("".getBytes(UTF_8))));

		check(servlet.serve(HttpRequest.get("http://some-test.com/a/b/c/d")), "", 200);
		check(servlet.serve(HttpRequest.post("http://some-test.com/a/b/c/d")), "", 200);
		check(servlet.serve(HttpRequest.of(HttpMethod.OPTIONS, "http://some-test.com/a/b/c/d")), "", 200);
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

		AsyncServlet action = request -> {
			ByteBuf msg = wrapUtf8("Executed: " + request.getPath());
			return Promise.of(HttpResponse.ofCode(200).withBody(msg));
		};

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
		check(main.serve(request1), "Executed: /", 200);
		check(main.serve(request2), "Executed: /a", 200);
		check(main.serve(request3), "Executed: /a/c", 200);
		check(main.serve(request4), "Executed: /a/d", 200);
		check(main.serve(request5), "", 404);
		check(main.serve(request6), "", 404);
		check(main.serve(request7), "Executed: /b/f", 200);
		check(main.serve(request8), "Executed: /b/g", 200);
		System.out.println();

		//		request5.recycleBufs();
		//		request6.recycleBufs();
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

		AsyncServlet action = request -> {
			ByteBuf msg = wrapUtf8("Executed: " + request.getPath());
			return Promise.of(HttpResponse.ofCode(200).withBody(msg));
		};

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/", action)
				.with(GET, "/a", action)
				.with(GET, "/a/c", action)
				.with(GET, "/a/d", action)
				.with(GET, "/b/f", action)
				.with(GET, "/b/g", action);

		System.out.println("Long mapping " + DELIM);
		check(main.serve(request1), "Executed: /", 200);
		check(main.serve(request2), "Executed: /a", 200);
		check(main.serve(request3), "Executed: /a/c", 200);
		check(main.serve(request4), "Executed: /a/d", 200);
		check(main.serve(request5), "", 404);
		check(main.serve(request6), "", 404);
		check(main.serve(request7), "Executed: /b/f", 200);
		check(main.serve(request8), "Executed: /b/g", 200);
		System.out.println();

		//		request5.recycleBufs();
		//		request6.recycleBufs();
	}

	@Test
	public void testOverrideHandler() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Can't map. Servlet already exists");

		MiddlewareServlet.create()
				.with(GET, "/", request -> null)
				.with(GET, "/", request -> null);
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

		AsyncServlet action = request -> {
			ByteBuf msg = wrapUtf8("Executed: " + request.getPath());
			return Promise.of(HttpResponse.ofCode(200).withBody(msg));
		};

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
		check(main.serve(request1), "Executed: /", 200);
		check(main.serve(request2), "Executed: /a", 200);
		check(main.serve(request3), "Executed: /b", 200);
		check(main.serve(request4), "Executed: /a/c", 200);
		check(main.serve(request5), "Executed: /a/d", 200);
		check(main.serve(request6), "Executed: /a/e", 200);
		check(main.serve(request7), "Executed: /a/c/f", 200);
		System.out.println();
	}

	@Test
	public void testFailMerge() {
		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/c/f");    // fail

		AsyncServlet action = req -> {
			ByteBuf msg = wrapUtf8("Executed: " + req.getPath());
			return Promise.of(HttpResponse.ofCode(200).withBody(msg));
		};

		AsyncServlet anotherAction = req -> {
			ByteBuf msg = wrapUtf8("Shall not be executed: " + req.getPath());
			return Promise.of(HttpResponse.ofCode(200).withBody(msg));
		};

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
			return;
		}

		check(main.serve(request), "SHALL NOT BE EXECUTED", 500);
	}

	@Test
	public void testParameter() {
		AsyncServlet printParameters = request -> {
			try {
				String body = request.getPathParameter("id")
						+ " " + request.getPathParameter("uid")
						+ " " + request.getPathParameter("eid");
				ByteBuf bodyByteBuf = wrapUtf8(body);
				return Promise.of(HttpResponse.ofCode(200).withBody(bodyByteBuf));
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/:id/a/:uid/b/:eid", printParameters)
				.with(GET, "/:id/a/:uid", printParameters);

		System.out.println("Parameter test " + DELIM);
		check(main.serve(HttpRequest.get("http://www.coursera.org/123/a/456/b/789")), "123 456 789", 200);
		Promise<HttpResponse> serve = main.serve(HttpRequest.get("http://www.coursera.org/555/a/777"));
		assertTrue(serve.materialize().getException() instanceof ParseException);
		HttpRequest request = HttpRequest.get("http://www.coursera.org");
		check(main.serve(request), "", 404);
		System.out.println();
	}

	@Test
	public void testMultiParameters() {
		MiddlewareServlet ms = MiddlewareServlet.create()
				.with(GET, "/serve/:cid/wash", request -> {
					try {
						ByteBuf body = wrapUtf8("served car: " + request.getPathParameter("cid"));
						return Promise.of(HttpResponse.ofCode(200).withBody(body));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/serve/:mid/feed", request -> {
					try {
						ByteBuf body = wrapUtf8("served man: " + request.getPathParameter("mid"));
						return Promise.of(HttpResponse.ofCode(200).withBody(body));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});

		System.out.println("Multi parameters " + DELIM);
		check(ms.serve(HttpRequest.get(TEMPLATE + "/serve/1/wash")), "served car: 1", 200);
		check(ms.serve(HttpRequest.get(TEMPLATE + "/serve/2/feed")), "served man: 2", 200);
		System.out.println();
	}

	@Test
	public void testDifferentMethods() {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/a/b/c/action");
		HttpRequest request2 = HttpRequest.post(TEMPLATE + "/a/b/c/action");
		HttpRequest request3 = HttpRequest.of(CONNECT, TEMPLATE + "/a/b/c/action");

		MiddlewareServlet servlet = MiddlewareServlet.create()
				.with("/a/b/c/action", request -> Promise.of(
						HttpResponse.ofCode(200).withBody(wrapUtf8("WILDCARD"))))
				.with(POST, "/a/b/c/action", request -> Promise.of(
						HttpResponse.ofCode(200).withBody(wrapUtf8("POST"))))
				.with(GET, "/a/b/c/action", request -> Promise.of(
						HttpResponse.ofCode(200).withBody(wrapUtf8("GET"))));

		System.out.println("Different methods " + DELIM);
		check(servlet.serve(request1), "GET", 200);
		check(servlet.serve(request2), "POST", 200);
		check(servlet.serve(request3), "WILDCARD", 200);
		System.out.println();
	}

	@Test
	public void testDefault() {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/html/admin/action");
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/html/admin/action/ban");

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/html/admin/action", request -> Promise.of(
						HttpResponse.ofCode(200).withBody(wrapUtf8("Action executed"))))
				.withFallback("/html/admin", request -> Promise.of(
						HttpResponse.ofCode(200).withBody(wrapUtf8("Stopped at admin: " + request.getRelativePath()))));

		System.out.println("Default stop " + DELIM);
		check(main.serve(request1), "Action executed", 200);
		check(main.serve(request2), "Stopped at admin: action/ban", 200);
		System.out.println();
	}

	@Test
	public void test404() {
		MiddlewareServlet main = MiddlewareServlet.create()
				.with("/a/:id/b/d", request -> Promise.of(
						HttpResponse.ofCode(200).withBody(wrapUtf8("All OK"))));

		System.out.println("404 " + DELIM);
		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/123/b/c");
		check(main.serve(request), "", 404);
		System.out.println();
	}

	@Test
	public void test405() {
		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/a/:id/b/d", request -> Promise.of(
						HttpResponse.ofCode(200).withBody(wrapUtf8("Should not execute"))));

		HttpRequest request = HttpRequest.post(TEMPLATE + "/a/123/b/d");
		check(main.serve(request), "", 404);
	}

	@Test
	public void test405WithFallback() {
		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/a/:id/b/d", request -> Promise.of(
						HttpResponse.ofCode(200).withBody(wrapUtf8("Should not execute"))))
				.withFallback("/a/:id/b/d", request -> Promise.of(
						HttpResponse.ofCode(200).withBody(wrapUtf8("Fallback executed"))));
		check(main.serve(HttpRequest.post(TEMPLATE + "/a/123/b/d")), "Fallback executed", 200);
	}

	@Test
	public void testFallbackTail() {
		AsyncServlet servlet = request -> Promise.of(HttpResponse.ofCode(200).withBody(wrapUtf8("Success: " + request.getRelativePath())));

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/method/:var", MiddlewareServlet.create().withFallback(servlet));

		check(main.serve(HttpRequest.get(TEMPLATE + "/method/byhjgngtgh/oneArg")), "Success: oneArg", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/aedscvv/first/second")), "Success: first/second", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/muimkik/")), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/fyju")), "Success: ", 200);
	}

	@Test(expected = AssertionError.class)
	public void testTailFail() {
		MiddlewareServlet.create().with(GET, "/method/:var*/:tail", request -> Promise.of(HttpResponse.ok200()));
	}

	@Test
	public void testTail() {
		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/method/:var/:tail*", request -> {
					try {
						ByteBuf body = wrapUtf8("Success: " + request.getPathParameter("tail"));
						return Promise.of(HttpResponse.ofCode(200).withBody(body));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});

		check(main.serve(HttpRequest.get(TEMPLATE + "/method/dfbdb/oneArg")), "Success: oneArg", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/srfethj/first/second")), "Success: first/second", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/dvyhju/")), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn")), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn?query=string")), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn/first?query=string")), "Success: first", 200);
	}
}
