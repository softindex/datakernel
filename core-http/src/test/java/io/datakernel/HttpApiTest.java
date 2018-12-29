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

package io.datakernel;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.http.HttpHeaderValue.*;
import static io.datakernel.http.HttpHeaders.*;
import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public final class HttpApiTest {
	public static final int PORT = 5568;

	private AsyncHttpServer server;
	private AsyncHttpClient client;

	// request
	private List<AcceptMediaType> requestAcceptContentTypes = new ArrayList<>();
	private List<AcceptCharset> requestAcceptCharsets = new ArrayList<>();
	private Instant requestDate = createDate(1999, 1, 1);
	private Instant dateIMS = createDate(2011, 3, 4);
	private Instant dateIUMS = createDate(2012, 5, 6);
	private MediaType requestMime = MediaTypes.ANY_TEXT;
	private ContentType requestContentType = ContentType.of(requestMime);
	private List<HttpCookie> requestCookies = new ArrayList<>();

	// response
	private Instant responseDate = createDate(2000, 11, 17);
	private Instant expiresDate = createDate(2011, 2, 22);
	private Instant lastModified = createDate(2099, 11, 13);
	private MediaType responseMime = MediaType.of("font/woff2");
	private Charset responseCharset = StandardCharsets.UTF_16LE;
	private ContentType responseContentType = ContentType.of(responseMime, responseCharset);
	private List<HttpCookie> responseCookies = new ArrayList<>();
	private int age = 10_000;

	@Before
	public void setUp() {
		server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
				request -> {
					testRequest(request);
					return Promise.of(createResponse());
				})
				.withListenPort(PORT);

		client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());

		// setup request and response data
		requestAcceptContentTypes.add(AcceptMediaType.of(MediaTypes.ANY_AUDIO, 90));
		requestAcceptContentTypes.add(AcceptMediaType.of(MediaTypes.ANY));
		requestAcceptContentTypes.add(AcceptMediaType.of(MediaTypes.ATOM));
		requestAcceptContentTypes.add(AcceptMediaType.of(MediaType.of("hello/world")));

		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("UTF-8")));
		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("ISO-8859-5"), 10));
		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("ISO-8859-2"), 10));
		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("ISO-8859-3"), 10));

		HttpCookie cookie2 = HttpCookie.of("name1", "value1");
		requestCookies.add(cookie2);
		HttpCookie cookie3 = HttpCookie.of("name3");
		requestCookies.add(cookie3);

		HttpCookie cookie1 = HttpCookie.of("name2", "value2");
		cookie1.setMaxAge(123);
		cookie1.setExpirationDate(Instant.now());
		responseCookies.add(cookie1);
	}

	@Test
	public void test() throws IOException {
		server.listen();
		await(client.request(createRequest())
				.whenComplete((response, e) -> {
					testResponse(response);
					server.close();
					client.stop();
				}));
	}

	private HttpResponse createResponse() {
		return HttpResponse.ok200()
				.withHeader(DATE, ofInstant(responseDate))
				.withHeader(EXPIRES, ofInstant(expiresDate))
				.withHeader(CONTENT_TYPE, ofContentType(responseContentType))
				.withHeader(LAST_MODIFIED, ofInstant(lastModified))
				.withHeader(AGE, ofDecimal(age))
				.withCookies(responseCookies);
	}

	private HttpRequest createRequest() {
		return HttpRequest.get("http://127.0.0.1:" + PORT)
				.withHeader(ACCEPT, ofAcceptMediaTypes(requestAcceptContentTypes))
				.withHeader(ACCEPT_CHARSET, ofAcceptCharsets(requestAcceptCharsets))
				.withHeader(DATE, ofInstant(requestDate))
				.withHeader(CONTENT_TYPE, ofContentType(requestContentType))
				.withHeader(IF_MODIFIED_SINCE, ofInstant(dateIMS))
				.withHeader(IF_UNMODIFIED_SINCE, ofInstant(dateIUMS))
				.initialize(httpRequest -> requestCookies.forEach(httpRequest::addCookie));
	}

	private void testResponse(HttpResponse response) {
		try {
			assertEquals(responseContentType, response.parseHeader(CONTENT_TYPE, HttpHeaderValue::toContentType));
			assertEquals(responseCookies, new ArrayList<>(response.getCookies().values()));
			assertEquals(responseDate, response.parseHeader(DATE, HttpHeaderValue::toInstant));
			assertEquals(age, (int) response.parseHeader(AGE, HttpHeaderValue::toPositiveInt));
			assertEquals(expiresDate, response.parseHeader(EXPIRES, HttpHeaderValue::toInstant));
			assertEquals(lastModified, response.parseHeader(LAST_MODIFIED, HttpHeaderValue::toInstant));
		} catch (ParseException e) {
			throw new AssertionError(e);
		}
	}

	private void testRequest(HttpRequest request) {
		try {
			assertEquals(requestAcceptContentTypes, request.parseHeader(ACCEPT, HttpHeaderValue::toAcceptContentTypes));
			assertEquals(requestAcceptCharsets, request.parseHeader(ACCEPT_CHARSET, HttpHeaderValue::toAcceptCharsets));
			assertEquals(requestDate, request.parseHeader(DATE, HttpHeaderValue::toInstant));
			assertEquals(dateIMS, request.parseHeader(IF_MODIFIED_SINCE, HttpHeaderValue::toInstant));
			assertEquals(dateIUMS, request.parseHeader(IF_UNMODIFIED_SINCE, HttpHeaderValue::toInstant));
			assertEquals(requestContentType, request.parseHeader(CONTENT_TYPE, HttpHeaderValue::toContentType));
			assertEquals(requestCookies.stream().map(HttpCookie::getValue).collect(toList()), new ArrayList<>(request.getCookies().values()));
		} catch (ParseException e) {
			throw new AssertionError(e);
		}
	}

	private static Instant createDate(int year, int month, int day) {
		return LocalDate.of(year, month, day).atStartOfDay().toInstant(UTC);
	}
}
