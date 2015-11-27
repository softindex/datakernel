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

public class Test {
	private static final int PORT = 1234;

	public static void main(String[] args) throws IOException {
		final NioEventloop eventloop = new NioEventloop();
		final AsyncHttpServer httpServerListener = helloWorldServer(eventloop, PORT);
		System.out.println("Started HelloWorld HTTP Server on http://localhost:" + PORT);
		httpServerListener.listen();

		final HttpClientImpl httpClient = new HttpClientImpl(eventloop,
				new NativeDnsResolver(eventloop, DatagramSocketSettings.defaultDatagramSocketSettings(),
						3_000L, InetAddresses.forString("8.8.8.8")));

		httpClient.getHttpResultAsync(createRequest(), 1000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {

//				System.out.println(ByteBufStrings.decodeAscii(result.write()));
//
//				System.out.println(result.getAge());
//				System.out.println(result.getContentLength());
//				System.out.println(result.getContentType());

				httpClient.close();
			}

			@Override
			public void onException(Exception exception) {
				httpClient.close();
			}
		});

		eventloop.run();
	}

	private static HttpRequest createRequest() {
		return HttpRequest.post("http://127.0.0.1:" + PORT)
				.setAccept(ContentType.JSON)
				.setAcceptCharset(Charset.forName("ISO-8859-1"))
				.setDate(new Date())
				.setContentType(ContentType.BMP)
				.cookie(Arrays.asList(new HttpCookie("token1", "dev"), new HttpCookie("token2")))
				.body(ByteBufStrings.wrapAscii("Hello, friend!"));
	}

	public static AsyncHttpServer helloWorldServer(NioEventloop primaryEventloop, int port) {
		AsyncHttpServer httpServer = new AsyncHttpServer(primaryEventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
				System.out.println(request.getAccept());
				System.out.println(request.getAcceptCharsets());
				System.out.println(request.getDate());
				System.out.println(request.getContentType());
				System.out.println(request.getCookies());

				HttpResponse response = HttpResponse.create()
						.setAge(10)
						.setContentLength(2010)
						.setContentType(ContentType.JAVASCRIPT_APP)
						.setDate(new Date())
						.setExpires(new Date())
						.setLastModified(new Date())
						.body("Hello!".getBytes());
				callback.onResult(response);
			}
		});
		return httpServer.setListenPort(port);
	}
}
