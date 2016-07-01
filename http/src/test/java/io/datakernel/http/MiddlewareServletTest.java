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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.util.ByteBufStrings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static org.junit.Assert.assertEquals;

public class MiddlewareServletTest {

	private static final String TEMPLATE = "http://www.site.org";
	private static final String DELIM = "*****************************************************************************";

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private static AsyncHttpServlet.Callback callback(final String expectedBody, final int expectedCode) {
		return new AsyncHttpServlet.Callback() {
			@Override
			public void onResult(HttpResponse result) {
				assertEquals(expectedBody, result.getBody() == null ? "" : result.getBody().toString());
				assertEquals(expectedCode, result.getCode());
				System.out.println(result + "  " + result.getBody());
				result.recycleBufs();
			}

			@Override
			public void onHttpError(HttpServletError httpServletError) {
			}
		};
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

		AsyncHttpServlet action = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				ByteBuf msg = ByteBufStrings.wrapUTF8("Executed: " + request.getPath());
				callback.onResult(HttpResponse.create(200).body(msg));
			}
		};

		MiddlewareServlet a = new MiddlewareServlet();
		a.get("/c", action);
		a.get("/d", action);
		a.get("/", action);

		MiddlewareServlet b = new MiddlewareServlet();
		b.get("/f", action);
		b.get("/g", action);

		MiddlewareServlet main = new MiddlewareServlet();
		main.get("/", action);
		main.get("/a", a);
		main.get("/b", b);

		System.out.println("Micro mapping" + DELIM);
		main.serveAsync(request1, callback("Executed: /", 200));
		main.serveAsync(request2, callback("Executed: /a", 200));
		main.serveAsync(request3, callback("Executed: /a/c", 200));
		main.serveAsync(request4, callback("Executed: /a/d", 200));
		main.serveAsync(request5, callback("", 404));
		main.serveAsync(request6, callback("", 404));
		main.serveAsync(request7, callback("Executed: /b/f", 200));
		main.serveAsync(request8, callback("Executed: /b/g", 200));
		System.out.println();
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

		AsyncHttpServlet action = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				ByteBuf msg = ByteBufStrings.wrapUTF8("Executed: " + request.getPath());
				callback.onResult(HttpResponse.create(200).body(msg));
			}
		};

		MiddlewareServlet main = new MiddlewareServlet();

		main.get("/", action);
		main.get("/a", action);
		main.get("/a/c", action);
		main.get("/a/d", action);
		main.get("/b/f", action);
		main.get("/b/g", action);

		System.out.println("Long mapping " + DELIM);
		main.serveAsync(request1, callback("Executed: /", 200));
		main.serveAsync(request2, callback("Executed: /a", 200));
		main.serveAsync(request3, callback("Executed: /a/c", 200));
		main.serveAsync(request4, callback("Executed: /a/d", 200));
		main.serveAsync(request5, callback("", 404));
		main.serveAsync(request6, callback("", 404));
		main.serveAsync(request7, callback("Executed: /b/f", 200));
		main.serveAsync(request8, callback("Executed: /b/g", 200));
		System.out.println();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testOverrideHandler() {
		AsyncHttpServlet action = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				ByteBuf msg = ByteBufStrings.wrapUTF8("Executed: " + request.getPath());
				callback.onResult(HttpResponse.create(200).body(msg));
			}
		};

		AsyncHttpServlet anotherAction = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				ByteBuf msg = ByteBufStrings.wrapUTF8("Executed: " + request.getPath());
				callback.onResult(HttpResponse.create(200).body(msg));
			}
		};

		MiddlewareServlet s1 = new MiddlewareServlet();
		s1.get("/", action);
		s1.get("/", action);

		expectedException.expect(RuntimeException.class);
		expectedException.expectMessage("Can't map. Handler already exists");
		s1.get("/", anotherAction);
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

		AsyncHttpServlet action = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				ByteBuf msg = ByteBufStrings.wrapUTF8("Executed: " + request.getPath());
				callback.onResult(HttpResponse.create(200).body(msg));
			}
		};

		MiddlewareServlet main = new MiddlewareServlet();
		main.get("/a", action);
		main.get("/a/c", action);
		main.get("/a/d", action);
		main.get("/b", action);

		MiddlewareServlet supp = new MiddlewareServlet();
		supp.get("/", action);
		supp.get("/a/e", action);
		supp.get("/a/c/f", action);

		main.get("/", supp);

		System.out.println("Merge   " + DELIM);
		main.serveAsync(request1, callback("Executed: /", 200));
		main.serveAsync(request2, callback("Executed: /a", 200));
		main.serveAsync(request3, callback("Executed: /b", 200));
		main.serveAsync(request4, callback("Executed: /a/c", 200));
		main.serveAsync(request5, callback("Executed: /a/d", 200));
		main.serveAsync(request6, callback("Executed: /a/e", 200));
		main.serveAsync(request7, callback("Executed: /a/c/f", 200));
		System.out.println();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testFailMerge() throws ParseException {
		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/c/f");    // fail

		AsyncHttpServlet action = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				ByteBuf msg = ByteBufStrings.wrapUTF8("Executed: " + request.getPath());
				callback.onResult(HttpResponse.create(200).body(msg));
			}
		};

		AsyncHttpServlet anotherAction = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				ByteBuf msg = ByteBufStrings.wrapUTF8("Shall not be executed: " + request.getPath());
				callback.onResult(HttpResponse.create(200).body(msg));
			}
		};

		MiddlewareServlet main = new MiddlewareServlet();
		main.get("/", action);
		main.get("/a/e", action);
		main.get("/a/c/f", action);

		MiddlewareServlet exc = new MiddlewareServlet();
		exc.get("/a/c/f", anotherAction);

		// /a/c/f already mapped
		expectedException.expect(RuntimeException.class);
		expectedException.expectMessage("Can't map. Handler for this method already exists");
		main.get("/", exc);

		main.serveAsync(request, callback("SHALL NOT BE EXECUTED", 500));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testParameter() throws ParseException {
		AsyncHttpServlet printParameters = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				HttpResponse response = HttpResponse.create(200);
				response.body(ByteBufStrings.wrapUTF8(request.getUrlParameter("id")
						+ " " + request.getUrlParameter("uid")
						+ " " + request.getUrlParameter("eid")));
				callback.onResult(response);
				request.recycleBufs();
			}
		};

		MiddlewareServlet main = new MiddlewareServlet();
		main.get("/:id/a/:uid/b/:eid", printParameters);
		main.get("/:id/a/:uid", printParameters);

		System.out.println("Parameter test " + DELIM);
		main.serveAsync(HttpRequest.get("http://www.coursera.org/123/a/456/b/789"), callback("123 456 789", 200));
		main.serveAsync(HttpRequest.get("http://www.coursera.org/555/a/777"), callback("555 777 null", 200));
		main.serveAsync(HttpRequest.get("http://www.coursera.org"), callback("", 404));
		System.out.println();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testMultiParameters() throws ParseException {
		AsyncHttpServlet serveCar = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				HttpResponse response = HttpResponse.create(200);
				response.body(ByteBufStrings.wrapUTF8("served car: " + request.getUrlParameter("cid")));
				callback.onResult(response);
			}
		};

		AsyncHttpServlet serveMan = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				HttpResponse response = HttpResponse.create(200);
				response.body(ByteBufStrings.wrapUTF8("served man: " + request.getUrlParameter("mid")));
				callback.onResult(response);
			}
		};

		MiddlewareServlet ms = new MiddlewareServlet();
		ms.get("/serve/:cid/wash", serveCar);
		ms.get("/serve/:mid/feed", serveMan);

		System.out.println("Multi parameters " + DELIM);
		ms.serveAsync(HttpRequest.get(TEMPLATE + "/serve/1/wash"), callback("served car: 1", 200));
		ms.serveAsync(HttpRequest.get(TEMPLATE + "/serve/2/feed"), callback("served man: 2", 200));
		System.out.println();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDifferentMethods() throws ParseException {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/a/b/c/action");
		HttpRequest request2 = HttpRequest.post(TEMPLATE + "/a/b/c/action");
		HttpRequest request3 = HttpRequest.create(HttpMethod.CONNECT).url(TEMPLATE + "/a/b/c/action");

		AsyncHttpServlet post = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				HttpResponse response = HttpResponse.create(200);
				response.body(ByteBufStrings.wrapUTF8("POST"));
				callback.onResult(response);
			}
		};

		AsyncHttpServlet get = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				HttpResponse response = HttpResponse.create(200);
				response.body(ByteBufStrings.wrapUTF8("GET"));
				callback.onResult(response);
			}
		};

		AsyncHttpServlet wildcard = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				HttpResponse response = HttpResponse.create(200);
				response.body(ByteBufStrings.wrapUTF8("WILDCARD"));
				callback.onResult(response);
			}
		};

		MiddlewareServlet servlet = new MiddlewareServlet();
		servlet.use("/a/b/c/action", wildcard);
		servlet.post("/a/b/c/action", post);
		servlet.get("/a/b/c/action", get);

		System.out.println("Different methods " + DELIM);
		servlet.serveAsync(request1, callback("GET", 200));
		servlet.serveAsync(request2, callback("POST", 200));
		servlet.serveAsync(request3, callback("WILDCARD", 200));
		System.out.println();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDefault() throws ParseException {
		AsyncHttpServlet def = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				callback.onResult(HttpResponse.create(200).body(ByteBufStrings.wrapUTF8("Stopped at admin: " + request.getRelativePath())));
			}
		};

		AsyncHttpServlet action = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				callback.onResult(HttpResponse.create(200).body(ByteBufStrings.wrapUTF8("Action executed")));
			}
		};

		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/html/admin/action");
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/html/admin/action/ban");

		MiddlewareServlet main = new MiddlewareServlet();
		main.get("/html/admin/action", action);
		main.setDefault("/html/admin", def);

		System.out.println("Default stop " + DELIM);
		main.serveAsync(request1, callback("Action executed", 200));
		main.serveAsync(request2, callback("Stopped at admin: /action/ban", 200));
		System.out.println();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void test404() throws ParseException {
		AsyncHttpServlet handler = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				callback.onResult(HttpResponse.create(200).body(ByteBufStrings.wrapUTF8("All OK")));
			}
		};
		MiddlewareServlet main = new MiddlewareServlet();
		main.use("/a/:id/b/c", null);
		main.use("/a/:id/b/d", handler);

		System.out.println("404 " + DELIM);
		main.serveAsync(HttpRequest.get(TEMPLATE + "/a/123/b/c"), callback("", 404));
		System.out.println();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}
