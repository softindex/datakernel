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

import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.GzipProcessorUtils.fromGzip;
import static io.datakernel.http.GzipProcessorUtils.toGzip;
import static io.datakernel.http.HttpHeaders.ACCEPT_ENCODING;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestGzipProcessorUtils {
	private static final int PORT = 5595;

	@Parameterized.Parameters
	public static Collection<Object[]> testData() {
		return Arrays.asList(new Object[][]{
				{"I"},
				{"I grant! I've never seen a goddess go."},
				{"I grant! I've never seen a goddess go. My mistress, when she walks, treads on the ground"},
				{generateLargeText()}
		});
	}

	@Parameterized.Parameter
	public String text;

	@Test
	public void testEncodeDecode() throws ParseException {
		ByteBuf raw = toGzip(wrapAscii(text));
		ByteBuf actual = fromGzip(raw);
		assertEquals(text, decodeAscii(actual));
		actual.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void recycleByteBufInCaseOfBadInput() {
		final ByteBuf badBuf = ByteBufPool.allocate(100);
		badBuf.put(new byte[]{-1, -1, -1, -1, -1, -1});

		try {
			fromGzip(badBuf);
			fail();
		} catch (ParseException ignored) {

		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testGzippedCommunicationBetweenClientServer() throws IOException, ParseException, ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				String receivedData = ByteBufStrings.decodeAscii(request.getBody());
				assertEquals("gzip", request.getHeader(HttpHeaders.CONTENT_ENCODING));
				assertEquals("gzip", request.getHeader(HttpHeaders.ACCEPT_ENCODING));
				assertEquals(text, receivedData);
				callback.setResult(HttpResponse.ok200().withBody(ByteBufStrings.wrapAscii(receivedData)));
			}
		};

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withDefaultGzipCompression(true)
				.withListenAddress(new InetSocketAddress("localhost", PORT));

		final AsyncHttpClient client = AsyncHttpClient.create(eventloop);

		final ResultCallbackFuture<String> callback = ResultCallbackFuture.create();

		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT)
				.withHeader(ACCEPT_ENCODING, "gzip")
				.withBody(wrapAscii(text), true);

		server.listen();
		client.send(request, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				assertEquals("gzip", result.getHeader(HttpHeaders.CONTENT_ENCODING));

				callback.setResult(decodeAscii(result.getBody()));
				server.close(IgnoreCompletionCallback.create());
				client.stop(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception e) {
				callback.setException(e);
				server.close(IgnoreCompletionCallback.create());
				client.stop(IgnoreCompletionCallback.create());
			}
		});

		eventloop.run();
		assertEquals(text, callback.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	// test response directives have higher priority then server
	@Test
	public void testServerDoesGzipResponseDoesNot() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				callback.setResult(HttpResponse.ok200()
						.withBody(request.detachBody(), false)
				);
			}
		};

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withDefaultGzipCompression(true)
				.withListenAddress(new InetSocketAddress("localhost", PORT));

		final AsyncHttpClient client = AsyncHttpClient.create(eventloop);

		final ResultCallbackFuture<String> callback = ResultCallbackFuture.create();

		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT)
				.withHeader(ACCEPT_ENCODING, "gzip")
				.withBody(wrapAscii(text), false);

		server.listen();
		client.send(request, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				assertNull(result.getHeader(HttpHeaders.CONTENT_ENCODING));

				callback.setResult(decodeAscii(result.getBody()));
				server.close(IgnoreCompletionCallback.create());
				client.stop(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception e) {
				callback.setException(e);
				server.close(IgnoreCompletionCallback.create());
				client.stop(IgnoreCompletionCallback.create());
			}
		});

		eventloop.run();
		assertEquals(text, callback.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testServerDoesNotGzipResponseDoes() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				callback.setResult(HttpResponse.ok200()
						.withBody(request.detachBody(), true)
				);
			}
		};

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withDefaultGzipCompression(false)
				.withListenAddress(new InetSocketAddress("localhost", PORT));

		final AsyncHttpClient client = AsyncHttpClient.create(eventloop);

		final ResultCallbackFuture<String> callback = ResultCallbackFuture.create();

		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT)
				.withHeader(ACCEPT_ENCODING, "gzip")
				.withBody(wrapAscii(text), false);

		server.listen();
		client.send(request, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				assertEquals("gzip", result.getHeader(HttpHeaders.CONTENT_ENCODING));

				callback.setResult(decodeAscii(result.getBody()));
				server.close(IgnoreCompletionCallback.create());
				client.stop(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception e) {
				callback.setException(e);
				server.close(IgnoreCompletionCallback.create());
				client.stop(IgnoreCompletionCallback.create());
			}
		});

		eventloop.run();
		assertEquals(text, callback.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDefaultServerBehaviour() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				callback.setResult(HttpResponse.ok200()
						.withBody(request.detachBody())
				);
			}
		};

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withDefaultGzipCompression(true)
				.withListenAddress(new InetSocketAddress("localhost", PORT));

		final AsyncHttpClient client = AsyncHttpClient.create(eventloop);

		final ResultCallbackFuture<String> callback = ResultCallbackFuture.create();

		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT)
				.withHeader(ACCEPT_ENCODING, "gzip")
				.withBody(wrapAscii(text));

		server.listen();
		client.send(request, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				assertNotNull(result.getHeader(HttpHeaders.CONTENT_ENCODING));

				callback.setResult(decodeAscii(result.getBody()));
				server.close(IgnoreCompletionCallback.create());
				client.stop(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception e) {
				callback.setException(e);
				server.close(IgnoreCompletionCallback.create());
				client.stop(IgnoreCompletionCallback.create());
			}
		});

		eventloop.run();
		assertEquals(text, callback.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testGzipInputStreamCorrectlyDecodesDataEncoded() throws IOException, ParseException {
		ByteBuf encodedData = toGzip(ByteBufStrings.wrapAscii(text));
		ByteBuf decoded = decodeWithGzipInputStream(encodedData);
		assertEquals(text, decodeAscii(decoded));
		encodedData.recycle();
		decoded.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testGzipOutputStreamDataIsCorrectlyDecoded() throws IOException, ParseException {
		ByteBuf encodedData = encodeWithGzipOutputStream(ByteBufStrings.wrapAscii(text));
		ByteBuf decoded = fromGzip(encodedData);
		assertEquals(text, decodeAscii(decoded));
		decoded.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private static ByteBuf encodeWithGzipOutputStream(ByteBuf raw) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
			gzip.write(raw.array(), raw.readPosition(), raw.readRemaining());
			gzip.finish();
			byte[] bytes = baos.toByteArray();
			ByteBuf byteBuf = ByteBufPool.allocate(bytes.length);
			byteBuf.put(bytes);
			return byteBuf;
		} finally {
			raw.recycle();
		}
	}

	private static ByteBuf decodeWithGzipInputStream(ByteBuf raw) throws IOException {
		try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(raw.array(), raw.readPosition(), raw.readRemaining()))) {
			int nRead;
			ByteBuf data = ByteBufPool.allocate(256);
			while ((nRead = gzip.read(data.array(), data.writePosition(), data.writeRemaining())) != -1) {
				data.moveWritePosition(nRead);
				data = ByteBufPool.ensureTailRemaining(data, data.readRemaining());
			}
			return data;
		}
	}

	private static String generateLargeText() {
		Random charRandom = new Random();
		int charactersCount = 10_000_000;
		StringBuilder sb = new StringBuilder(charactersCount);
		for (int i = 0; i < charactersCount; i++) {
			int charCode = charRandom.nextInt(255);
			sb.append((char) charCode);
		}
		return sb.toString();
	}
}
