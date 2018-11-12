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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static io.datakernel.bytebuf.ByteBufStrings.asAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.AsyncServlet.ensureRequestBody;
import static io.datakernel.http.GzipProcessorUtils.fromGzip;
import static io.datakernel.http.GzipProcessorUtils.toGzip;
import static io.datakernel.http.HttpHeaders.ACCEPT_ENCODING;
import static io.datakernel.http.IAsyncHttpClient.ensureResponseBody;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestGzipProcessorUtils {
	private static final int PORT = 5595;

	@Parameters
	public static Collection<Object[]> testData() {
		return Arrays.asList(new Object[][]{
				{"I"},
				{"I grant! I've never seen a goddess go."},
				{"I grant! I've never seen a goddess go. My mistress, when she walks, treads on the ground"},
				{generateLargeText()}
		});
	}

	@Parameter
	public String text;

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testEncodeDecode() throws ParseException {
		ByteBuf raw = toGzip(wrapAscii(text));
		ByteBuf actual = fromGzip(raw, 11_000_000);
		assertEquals(text, asAscii(actual));
	}

	@Test
	public void testEncodeDecodeWithTrailerInputSizeLessThenActual() throws ParseException {
		expectedException.expect(ParseException.class);
		expectedException.expectMessage("Decompressed data size is not equal to input size from GZIP trailer");
		ByteBuf raw = toGzip(wrapAscii(text));
		raw.moveWritePosition(-4);
		raw.writeInt(Integer.reverseBytes(2));
		fromGzip(raw, 11_000_000);
	}

	@Test
	public void recycleByteBufInCaseOfBadInput() {
		ByteBuf badBuf = ByteBufPool.allocate(100);
		badBuf.put(new byte[]{-1, -1, -1, -1, -1, -1});

		try {
			fromGzip(badBuf, 11_000_000);
			fail();
		} catch (ParseException ignored) {

		}
	}

	@Test
	public void testGzippedCommunicationBetweenClientServer() throws IOException, ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				ensureRequestBody(request -> {
					ByteBuf buf = request.getBody();
					String receivedData = asAscii(buf);
					assertEquals("gzip", request.getHeader(HttpHeaders.CONTENT_ENCODING));
					assertEquals("gzip", request.getHeader(HttpHeaders.ACCEPT_ENCODING));
					assertEquals(text, receivedData);
					return Promise.of(HttpResponse.ok200()
							.withBodyGzipCompression()
							.withBody(ByteBufStrings.wrapAscii(receivedData)));
				}))
				.withListenAddress(new InetSocketAddress("localhost", PORT));

		AsyncHttpClient client = AsyncHttpClient.create(eventloop);

		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT)
				.withHeader(ACCEPT_ENCODING, "gzip")
				.withBodyGzipCompression()
				.withBody(wrapAscii(text));

		server.listen();
		CompletableFuture<String> future = client.request(request)
				.thenCompose(ensureResponseBody())
				.thenApply(result -> {
					assertEquals("gzip", result.getHeaderOrNull(HttpHeaders.CONTENT_ENCODING));
					server.close();
					client.stop();
					ByteBuf buf = result.getBody();
					return asAscii(buf);
				})
				.toCompletableFuture();

		eventloop.run();
		assertEquals(text, future.get());
	}

	@Test
	public void testGzipInputStreamCorrectlyDecodesDataEncoded() throws IOException {
		ByteBuf encodedData = toGzip(ByteBufStrings.wrapAscii(text));
		ByteBuf decoded = decodeWithGzipInputStream(encodedData);
		assertEquals(text, asAscii(decoded));
		encodedData.recycle();
	}

	@Test
	public void testGzipOutputStreamDataIsCorrectlyDecoded() throws IOException, ParseException {
		ByteBuf encodedData = encodeWithGzipOutputStream(ByteBufStrings.wrapAscii(text));
		ByteBuf decoded = fromGzip(encodedData, 11_000_000);
		assertEquals(text, asAscii(decoded));
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
				data = ByteBufPool.ensureWriteRemaining(data, data.readRemaining());
			}
			return data;
		}
	}

	private static String generateLargeText() {
		Random charRandom = new Random(1L);
		int charactersCount = 10_000_000;
		StringBuilder sb = new StringBuilder(charactersCount);
		for (int i = 0; i < charactersCount; i++) {
			int charCode = charRandom.nextInt(255);
			sb.append((char) charCode);
		}
		return sb.toString();
	}
}
