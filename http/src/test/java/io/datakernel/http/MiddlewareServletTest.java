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

import io.datakernel.async.SettableStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.exception.ParseException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.function.BiConsumer;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.http.HttpMethod.*;
import static org.junit.Assert.assertEquals;

public class MiddlewareServletTest {

	private static final String TEMPLATE = "http://www.site.org";
	private static final String DELIM = "*****************************************************************************";
	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	}

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private static BiConsumer<HttpResponse, Throwable> assertResult(String expectedBody, int expectedCode) {
		return (result, throwable) -> {
			if (throwable == null) {
				assertEquals(expectedBody, result.getBody() == null ? "" : result.getBody().toString());
				assertEquals(expectedCode, result.getCode());
				System.out.println(result + "  " + result.getBody());
				result.recycleBufs();
			} else {
				assertEquals(expectedCode, ((HttpException) throwable).getCode());
			}
		};
	}

	@Test
	public void testBase() {
		MiddlewareServlet servlet1 = MiddlewareServlet.create();
		servlet1.with(HttpMethod.GET, "/a/b/c", request -> SettableStage.immediateStage(HttpResponse.ofCode(200)));

		servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c")).whenComplete(assertResult("", 200));
		servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c/d")).whenComplete(assertResult("", 404));
		servlet1.serve(HttpRequest.post("http://some-test.com/a/b/c")).whenComplete(assertResult("", 405));

		MiddlewareServlet servlet2 = MiddlewareServlet.create();
		servlet2.with(HttpMethod.HEAD, "/a/b/c", request -> SettableStage.immediateStage(HttpResponse.ofCode(200)));

		servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c")).whenComplete(assertResult("", 405));
		servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c/d")).whenComplete(assertResult("", 404));
		servlet2.serve(HttpRequest.of(HttpMethod.HEAD, "http://some-test.com/a/b/c")).whenComplete(assertResult("", 200));
	}

	@Test
	public void testProcessWildCardRequest() {
		MiddlewareServlet servlet = MiddlewareServlet.create();
		servlet.with("/a/b/c/d", request -> SettableStage.immediateStage(HttpResponse.ofCode(200)));

		servlet.serve(HttpRequest.get("http://some-test.com/a/b/c/d")).whenComplete(assertResult("", 200));
		servlet.serve(HttpRequest.post("http://some-test.com/a/b/c/d")).whenComplete(assertResult("", 200));
		servlet.serve(HttpRequest.of(HttpMethod.OPTIONS, "http://some-test.com/a/b/c/d")).whenComplete(assertResult("", 200));
	}

	@Test
	public void testMicroMapping() throws ParseException {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/");     // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a");    // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/a/c");  // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/d");  // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/e");  // 404
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/b");    // 404
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/b/f");  // ok
		HttpRequest request8 = HttpRequest.get(TEMPLATE + "/b/g");  // ok

		AsyncServlet action = request -> {
			ByteBuf msg = ByteBufStrings.wrapUtf8("Executed: " + request.getPath());
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(msg));
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
		main.serve(request1).whenComplete(assertResult("Executed: /", 200));
		main.serve(request2).whenComplete(assertResult("Executed: /a", 200));
		main.serve(request3).whenComplete(assertResult("Executed: /a/c", 200));
		main.serve(request4).whenComplete(assertResult("Executed: /a/d", 200));
		main.serve(request5).whenComplete(assertResult("", 404));
		main.serve(request6).whenComplete(assertResult("", 404));
		main.serve(request7).whenComplete(assertResult("Executed: /b/f", 200));
		main.serve(request8).whenComplete(assertResult("Executed: /b/g", 200));
		System.out.println();
		eventloop.run();

		request5.recycleBufs();
		request6.recycleBufs();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testLongMapping() throws ParseException {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/");     // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a");    // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/a/c");  // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/d");  // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/e");  // 404
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/b");    // 404
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/b/f");  // ok
		HttpRequest request8 = HttpRequest.get(TEMPLATE + "/b/g");  // ok

		AsyncServlet action = request -> {
			ByteBuf msg = ByteBufStrings.wrapUtf8("Executed: " + request.getPath());
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(msg));
		};

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/", action)
				.with(GET, "/a", action)
				.with(GET, "/a/c", action)
				.with(GET, "/a/d", action)
				.with(GET, "/b/f", action)
				.with(GET, "/b/g", action);

		System.out.println("Long mapping " + DELIM);
		main.serve(request1).whenComplete(assertResult("Executed: /", 200));
		main.serve(request2).whenComplete(assertResult("Executed: /a", 200));
		main.serve(request3).whenComplete(assertResult("Executed: /a/c", 200));
		main.serve(request4).whenComplete(assertResult("Executed: /a/d", 200));
		main.serve(request5).whenComplete(assertResult("", 404));
		main.serve(request6).whenComplete(assertResult("", 404));
		main.serve(request7).whenComplete(assertResult("Executed: /b/f", 200));
		main.serve(request8).whenComplete(assertResult("Executed: /b/g", 200));
		System.out.println();
		eventloop.run();

		request5.recycleBufs();
		request6.recycleBufs();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testOverrideHandler() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Can't map. Servlet already exists");

		MiddlewareServlet s1 = MiddlewareServlet.create()
				.with(GET, "/", request -> SettableStage.immediateStage(null))
				.with(GET, "/", request -> SettableStage.immediateStage(null));

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testMerge() throws ParseException {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/");         // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a");        // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/b");        // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/c");      // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/d");      // ok
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/a/e");      // ok
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/a/c/f");    // ok

		AsyncServlet action = request -> {
			ByteBuf msg = ByteBufStrings.wrapUtf8("Executed: " + request.getPath());
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(msg));
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
		main.serve(request1).whenComplete(assertResult("Executed: /", 200));
		main.serve(request2).whenComplete(assertResult("Executed: /a", 200));
		main.serve(request3).whenComplete(assertResult("Executed: /b", 200));
		main.serve(request4).whenComplete(assertResult("Executed: /a/c", 200));
		main.serve(request5).whenComplete(assertResult("Executed: /a/d", 200));
		main.serve(request6).whenComplete(assertResult("Executed: /a/e", 200));
		main.serve(request7).whenComplete(assertResult("Executed: /a/c/f", 200));
		System.out.println();
		eventloop.run();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testFailMerge() throws ParseException {
		AsyncServlet action = request -> {
			ByteBuf msg = ByteBufStrings.wrapUtf8("Executed: " + request.getPath());
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(msg));
		};

		AsyncServlet anotherAction = request -> {
			ByteBuf msg = ByteBufStrings.wrapUtf8("Shall not be executed: " + request.getPath());
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(msg));
		};

		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/c/f");    // fail
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
			assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
			return;
		}

		main.serve(request).whenComplete(assertResult("SHALL NOT BE EXECUTED", 500));
		eventloop.run();
		request.recycleBufs();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testParameter() throws ParseException {
		AsyncServlet printParameters = request -> {
			String body = request.getPathParameter("id")
					+ " " + request.getPathParameter("uid")
					+ " " + request.getPathParameter("eid");
			ByteBuf bodyByteBuf = ByteBufStrings.wrapUtf8(body);
			final HttpResponse httpResponse = HttpResponse.ofCode(200).withBody(bodyByteBuf);
			request.recycleBufs();
			return SettableStage.immediateStage(httpResponse);
		};

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/:id/a/:uid/b/:eid", printParameters)
				.with(GET, "/:id/a/:uid", printParameters);

		System.out.println("Parameter test " + DELIM);
		main.serve(HttpRequest.get("http://www.coursera.org/123/a/456/b/789")).whenComplete(assertResult("123 456 789", 200));
		main.serve(HttpRequest.get("http://www.coursera.org/555/a/777")).whenComplete(assertResult("555 777 null", 200));
		final HttpRequest request = HttpRequest.get("http://www.coursera.org");
		main.serve(request).whenComplete(assertResult("", 404));
		System.out.println();
		eventloop.run();
		request.recycleBufs();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testMultiParameters() throws ParseException {
		AsyncServlet serveCar = request -> {
			ByteBuf body = ByteBufStrings.wrapUtf8("served car: " + request.getPathParameter("cid"));
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(body));
		};

		AsyncServlet serveMan = request -> {
			ByteBuf body = ByteBufStrings.wrapUtf8("served man: " + request.getPathParameter("mid"));
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(body));
		};

		MiddlewareServlet ms = MiddlewareServlet.create()
				.with(GET, "/serve/:cid/wash", serveCar)
				.with(GET, "/serve/:mid/feed", serveMan);

		System.out.println("Multi parameters " + DELIM);
		ms.serve(HttpRequest.get(TEMPLATE + "/serve/1/wash")).whenComplete(assertResult("served car: 1", 200));
		ms.serve(HttpRequest.get(TEMPLATE + "/serve/2/feed")).whenComplete(assertResult("served man: 2", 200));
		System.out.println();
		eventloop.run();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDifferentMethods() throws ParseException {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/a/b/c/action");
		HttpRequest request2 = HttpRequest.post(TEMPLATE + "/a/b/c/action");
		HttpRequest request3 = HttpRequest.of(CONNECT, TEMPLATE + "/a/b/c/action");

		AsyncServlet post = request -> {
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("POST")));
		};

		AsyncServlet get = request -> {
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("GET")));
		};

		AsyncServlet wildcard = request -> {
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("WILDCARD")));
		};

		MiddlewareServlet servlet = MiddlewareServlet.create()
				.with("/a/b/c/action", wildcard)
				.with(POST, "/a/b/c/action", post)
				.with(GET, "/a/b/c/action", get);

		System.out.println("Different methods " + DELIM);
		servlet.serve(request1).whenComplete(assertResult("GET", 200));
		servlet.serve(request2).whenComplete(assertResult("POST", 200));
		servlet.serve(request3).whenComplete(assertResult("WILDCARD", 200));
		System.out.println();
		eventloop.run();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDefault() throws ParseException {
		AsyncServlet def = request -> {
			final ByteBuf body = ByteBufStrings.wrapUtf8("Stopped at admin: " + request.getPartialPath());
			request.recycleBufs();
			return SettableStage.immediateStage(
					HttpResponse.ofCode(200).withBody(body));
		};

		AsyncServlet action = request -> {
			request.recycleBufs();
			return SettableStage.immediateStage(
					HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("Action executed")));
		};

		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/html/admin/action");
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/html/admin/action/ban");

		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/html/admin/action", action)
				.withFallback("/html/admin", def);

		System.out.println("Default stop " + DELIM);
		main.serve(request1).whenComplete(assertResult("Action executed", 200));
		main.serve(request2).whenComplete(assertResult("Stopped at admin: /action/ban", 200));
		System.out.println();
		eventloop.run();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void test404() throws ParseException {
		AsyncServlet servlet = request -> {
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("All OK")));
		};
		MiddlewareServlet main = MiddlewareServlet.create()
				.with("/a/:id/b/d", servlet);

		System.out.println("404 " + DELIM);
		final HttpRequest request = HttpRequest.get(TEMPLATE + "/a/123/b/c");
		main.serve(request).whenComplete(assertResult("", 404));
		System.out.println();
		eventloop.run();
		request.recycleBufs();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void test405() throws ParseException {
		AsyncServlet servlet = request -> {
			request.recycleBufs();
			return SettableStage.immediateStage(HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("Should not execute")));
		};
		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/a/:id/b/d", servlet);

		final HttpRequest request = HttpRequest.post(TEMPLATE + "/a/123/b/d");
		main.serve(request).whenComplete(assertResult("", 405));
		eventloop.run();
		request.recycleBufs();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void test405WithFallback() {
		AsyncServlet servlet = request -> {
			request.recycleBufs();
			return SettableStage.immediateStage(
					HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("Should not execute")));
		};
		AsyncServlet fallback = request -> {
			request.recycleBufs();
			return SettableStage.immediateStage(
					HttpResponse.ofCode(200).withBody(ByteBufStrings.wrapUtf8("Fallback executed")));
		};
		MiddlewareServlet main = MiddlewareServlet.create()
				.with(GET, "/a/:id/b/d", servlet)
				.withFallback("/a/:id/b/d", fallback);
		main.serve(HttpRequest.post(TEMPLATE + "/a/123/b/d")).whenComplete(assertResult("Fallback executed", 200));
		eventloop.run();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}
