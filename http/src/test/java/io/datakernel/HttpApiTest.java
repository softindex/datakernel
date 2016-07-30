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

package io.datakernel;

import io.datakernel.async.ParseException;
import io.datakernel.async.ResultCallback;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings("ConstantConditions")
public class HttpApiTest {
	public static final int PORT = 5568;

	private Eventloop eventloop;
	private AsyncHttpServer server;
	private AsyncHttpClient client;

	// request
	private List<AcceptMediaType> requestAcceptContentTypes = new ArrayList<>();
	private List<AcceptCharset> requestAcceptCharsets = new ArrayList<>();
	private Date requestDate = createDate(1999, 1, 1);
	private Date dateIMS = createDate(2012, 18, 1);
	private Date dateIUMS = createDate(1972, 0, 1);
	private MediaType requestMime = MediaTypes.ANY_TEXT;
	private ContentType requestContentType = ContentType.of(requestMime);
	private List<HttpCookie> requestCookies = new ArrayList<>();

	// response
	private Date responseDate = createDate(2000, 11, 17);
	private Date expiresDate = createDate(2011, 2, 22);
	private Date lastModified = createDate(2099, 11, 13);
	private MediaType responseMime = MediaType.of("font/woff2");
	private Charset responseCharset = StandardCharsets.UTF_16LE;
	private ContentType responseContentType = ContentType.of(responseMime, responseCharset);
	private List<HttpCookie> responseCookies = new ArrayList<>();
	private int age = 10_000;

	@Before
	public void setUp() {
		eventloop = new Eventloop();
		server = new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) throws ParseException {
				testRequest(request);
				HttpResponse response = createResponse();
				callback.onResult(response);
			}
		}).setListenPort(PORT);

		client = new AsyncHttpClient(eventloop, new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS,
				3_000L, HttpUtils.inetAddress("8.8.8.8")));

		// setup request and response data
		requestAcceptContentTypes.add(AcceptMediaType.of(MediaTypes.ANY_AUDIO, 90));
		requestAcceptContentTypes.add(AcceptMediaType.of(MediaTypes.ANY));
		requestAcceptContentTypes.add(AcceptMediaType.of(MediaTypes.ATOM));
		requestAcceptContentTypes.add(AcceptMediaType.of(MediaType.of("hello/world")));

		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("UTF-8")));
		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("ISO-8859-5"), 10));
		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("ISO-8859-2"), 10));
		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("ISO-8859-3"), 10));

		HttpCookie cookie2 = new HttpCookie("name1", "value1");
		requestCookies.add(cookie2);
		HttpCookie cookie3 = new HttpCookie("name3");
		requestCookies.add(cookie3);

		HttpCookie cookie1 = new HttpCookie("name2", "value2");
		cookie1.setMaxAge(123);
		cookie1.setExpirationDate(new Date());
		responseCookies.add(cookie1);
	}

	@Test
	public void test() throws IOException, ExecutionException, InterruptedException {
		server.listen();
		HttpRequest request = createRequest();
		client.send(request, 1000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				try {
					testResponse(result);
				} catch (ParseException e) {
					fail("Invalid response");
				}
				server.close();
				client.close();
			}

			@Override
			public void onException(Exception e) {
				fail("Should not end here");
				server.close();
				client.close();
			}
		});
		eventloop.run();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private HttpResponse createResponse() {
		HttpResponse response = HttpResponse.create();
		response.date(responseDate);
		response.expires(expiresDate);
		response.contentType(responseContentType);
		response.setCookies(responseCookies);
		response.lastModified(lastModified);
		response.age(age);
		return response;
	}

	private HttpRequest createRequest() {
		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT);
		request.accept(requestAcceptContentTypes);
		request.acceptCharsets(requestAcceptCharsets);
		request.date(requestDate);
		request.contentType(requestContentType);
		request.ifModifiedSince(dateIMS);
		request.ifUnModifiedSince(dateIUMS);
		request.cookies(requestCookies);
		return request;
	}

	private void testResponse(HttpResponse response) throws ParseException {
		assertEquals(responseContentType.toString(), response.getContentType().toString());
		assertEquals(responseCookies.toString(), response.parseCookies().toString());
		assertEquals(responseDate.toString(), response.getDate().toString());
		assertEquals(age, response.parseAge());
		assertEquals(expiresDate.toString(), response.parseExpires().toString());
		assertEquals(lastModified.toString(), response.parseLastModified().toString());
	}

	private void testRequest(HttpRequest request) throws ParseException {
		assertEquals(requestAcceptContentTypes.toString(), request.parseAccept().toString());
		assertEquals(requestAcceptCharsets.toString(), request.parseAcceptCharsets().toString());
		assertEquals(requestDate.toString(), request.getDate().toString());
		assertEquals(dateIMS.toString(), request.parseIfModifiedSince().toString());
		assertEquals(dateIUMS.toString(), request.parseIfUnModifiedSince().toString());
		assertEquals(requestContentType.toString(), request.getContentType().toString());
		assertEquals(requestCookies.toString(), request.parseCookies().toString());
	}

	private static Date createDate(int year, int month, int day) {
		return new GregorianCalendar(year, month, day).getTime();
	}
}
