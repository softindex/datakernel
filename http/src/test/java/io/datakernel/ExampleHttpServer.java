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

import com.google.common.net.InetAddresses;
import io.datakernel.async.ResultCallback;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.*;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.util.ByteBufStrings;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class ExampleHttpServer {
	private static final int PORT = 1234;

	public static void main(String[] args) throws IOException {
		final NioEventloop eventloop = new NioEventloop();
		final AsyncHttpServer server = createServer(eventloop, PORT);

		System.out.println("Started HelloWorld HTTP Server on http://localhost:" + PORT);
		server.listen();

		final HttpClientImpl httpClient = new HttpClientImpl(eventloop,
				new NativeDnsResolver(eventloop, DatagramSocketSettings.defaultDatagramSocketSettings(),
						3_000L, InetAddresses.forString("8.8.8.8")));

		httpClient.getHttpResultAsync(createRequest(), 1000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				processResponse(result);
				server.close();
				httpClient.close();
			}

			@Override
			public void onException(Exception e) {
				server.close();
				httpClient.close();
			}
		});
		eventloop.run();
	}

	public static AsyncHttpServer createServer(NioEventloop primaryEventloop, int port) {
		return new AsyncHttpServer(primaryEventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
				callback.onResult(processRequest(request));
			}
		}).setListenPort(port);
	}

	private static HttpRequest createRequest() {
		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT)
				.setAccept(Arrays.asList(ContentType.XHTML_APP, ContentType.AAC, ContentType.ANY_APPLICATION))
				.setAcceptCharset(Charset.forName("ISO-8859-1"))
				.setDate(new Date())
				.setContentType(ContentType.BMP)
				.cookie(Arrays.asList(new HttpCookie("token1", "dev"), new HttpCookie("token2")))
				.body(ByteBufStrings.wrapAscii("Hello, friend! This is HttpRequest!"));

		System.out.println("\nFormed request on the client side: ");
		System.out.println(ByteBufStrings.decodeAscii(request.write()));
		System.out.println();

		return request;
	}

	private static HttpResponse processRequest(HttpRequest request) {
		System.out.println("Received request on the server side: ");
		System.out.println(ByteBufStrings.decodeAscii(request.write()));

		System.out.println("\nExtracted data: ");
		System.out.println("Accepts: " + request.getAccept());
		System.out.println("Content Type: " + request.getContentType());
		System.out.println("Content Length: " + request.getContentType());
		System.out.println("Accept Charsets: " + request.getAcceptCharsets());
		System.out.println("Date: " + request.getDate());
		System.out.println("Cookie: " + request.getCookies());
		System.out.println();

		return HttpResponse.create()
				.setContentType(ContentType.HTML)
				.setDate(new Date())
				.setCookie(Arrays.asList(new HttpCookie("test"), new HttpCookie("hello", "world")))
				.setCookie(new HttpCookie("single"))
				.setCookie(new HttpCookie("dikaya", "roza"))
				.body("<html><head></head><body>Hello!</body></html>!".getBytes());
	}

	private static void processResponse(HttpResponse response) {
		System.out.println("Received response from server: ");
		System.out.println(ByteBufStrings.decodeAscii(response.write()));
		System.out.println("\nExtracted data: ");
		System.out.println("Content type: " + response.getContentTypes());
		System.out.println("Date: " + response.getDate());
		List<HttpCookie> cookies = response.getCookies();
		for (HttpCookie cookie : cookies) {
			System.out.println(cookie);
		}

	}
}
