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

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Future;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.TestUtils.readFully;
import static io.datakernel.http.TestUtils.toByteArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HttpTolerantApplicationTest {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	public static AsyncHttpServer asyncHttpServer(Eventloop primaryEventloop, int port) {

		return AsyncHttpServer.create(primaryEventloop,
				request ->
						Stage.ofCallback(cb ->
								primaryEventloop.post(() -> cb.set(
										HttpResponse.ok200()
												.withBody(encodeAscii(request.getUrl().getPathAndQuery()))))))
				.withListenAddress(new InetSocketAddress("localhost", port));
	}

	private static void write(Socket socket, String string) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(encodeAscii(string));
		socket.getOutputStream().write(buf.array(), buf.readPosition(), buf.readRemaining());
	}

	private static void readAndAssert(InputStream is, String expected) throws IOException {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		assertEquals(expected, decodeAscii(bytes));
	}

	@Test
	public void testTolerantServer() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		AsyncHttpServer server = asyncHttpServer(eventloop, port);
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
		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	private static ServerSocket socketServer(int port, String testResponse) throws IOException {
		ServerSocket listener = new ServerSocket(port);
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					try (Socket socket = listener.accept()) {
						System.out.println("accept: " + socket);
						DataInputStream in = new DataInputStream(socket.getInputStream());
						readHttpMessage(in);
						System.out.println("write: " + socket);
						write(socket, testResponse);
					} catch (IOException ignored) {
					}
				}
			}

			public void readHttpMessage(DataInputStream in) throws IOException {
				//noinspection StatementWithEmptyBody
				int eofCounter = 0;
				while (eofCounter < 2) {
					int i = in.read();
					if (i == -1)
						break;
					if (i == LF) {
						eofCounter++;
					} else {
						if (i != CR)
							eofCounter = 0;
					}
				}
			}
		}).start();
		return listener;
	}

	@Test
	public void testTolerantClient() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		try (ServerSocket ignored = socketServer(port, "HTTP/1.1 200 OK\nContent-Type:  \t  text/html; charset=UTF-8\nContent-Length:  4\n\n/abc")) {
			AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop);

			Future<String> future = httpClient.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get("http://127.0.0.1:" + port))
					.thenApply(response ->
							response.getHeaderOrNull(HttpHeaders.CONTENT_TYPE))
					.thenRunEx(httpClient::stop)
					.toCompletableFuture();

			eventloop.run();

			assertEquals("text/html; charset=UTF-8", future.get());
		}
	}

}
