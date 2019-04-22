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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashSet;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.TestUtils.readFully;
import static io.datakernel.http.TestUtils.toByteArray;
import static io.datakernel.test.TestUtils.asserting;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public final class HttpTolerantApplicationTest {

	@Test
	public void testTolerantServer() throws Exception {
		int port = getFreePort();

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				request ->
						Promise.ofCallback(cb ->
								eventloop.post(() -> cb.set(
										HttpResponse.ok200()
												.withBody(encodeAscii(request.getUrl().getPathAndQuery()))))))
				.withListenPort(port);

		server.listen();

		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress("localhost", port));
		write(socket, "GET /abc  HTTP/1.1\nHost: \tlocalhost\n\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
		write(socket, "GET /abc  HTTP/1.0\nHost: \tlocalhost \t \nConnection: keep-alive\n\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
		write(socket, "GET /abc  HTTP1.1\nHost: \tlocalhost \t \n\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\n/abc");
		assertEquals(0, toByteArray(socket.getInputStream()).length);
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	@Test
	public void testTolerantClient() throws Exception {
		int port = getFreePort();

		ServerSocket listener = new ServerSocket(port);
		new Thread(() -> {
			while (Thread.currentThread().isAlive()) {
				try (Socket socket = listener.accept()) {
					System.out.println("accept: " + socket);
					DataInputStream in = new DataInputStream(socket.getInputStream());
					int b = 0;
					while (b != -1 && !(((b = in.read()) == CR || b == LF) && (b = in.read()) == LF)) {
					}
					System.out.println("write: " + socket);
					write(socket, "HTTP/1.1 200 OK\nContent-Type:  \t  text/html; charset=UTF-8\nContent-Length:  4\n\n/abc");
				} catch (IOException ignored) {
				}
			}
		})
				.start();

		String header = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.get("http://127.0.0.1:" + port))
				.map(response -> response.getHeaderOrNull(HttpHeaders.CONTENT_TYPE))
				.whenComplete(asserting(($, e) -> {
					listener.close();
				})));

		assertEquals("text/html; charset=UTF-8", header);
	}

	private static void write(Socket socket, String string) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(encodeAscii(string));
		socket.getOutputStream().write(buf.array(), buf.head(), buf.readRemaining());
	}

	private static void readAndAssert(InputStream is, String expected) {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		String actual = decodeAscii(bytes);
		assertEquals(new LinkedHashSet<>(asList(expected.split("\r\n"))), new LinkedHashSet<>(asList(actual.split("\r\n"))));
	}
}
