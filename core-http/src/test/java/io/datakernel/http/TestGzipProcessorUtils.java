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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.common.exception.parse.ParseException;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.GzipProcessorUtils.fromGzip;
import static io.datakernel.http.GzipProcessorUtils.toGzip;
import static io.datakernel.http.HttpHeaders.ACCEPT_ENCODING;
import static io.datakernel.http.HttpHeaders.CONTENT_ENCODING;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertEquals;

@RunWith(Parameterized.class)
public final class TestGzipProcessorUtils {
	private static final int PORT = getFreePort();
	public static final int CHARACTERS_COUNT = 10_000_000;

	@Parameters
	public static List<String> testData() {
		return Arrays.asList(
				"I",
				"I grant! I've never seen a goddess go.",
				"I grant! I've never seen a goddess go. My mistress, when she walks, treads on the ground",
				generateLargeText()
		);
	}

	@Parameter
	public String text;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Test
	public void testEncodeDecode() throws ParseException {
		ByteBuf raw = toGzip(wrapUtf8(text));
		ByteBuf actual = fromGzip(raw, 11_000_000);
		assertEquals(text, actual.asString(UTF_8));
	}

	@Test
	public void testEncodeDecodeWithTrailerInputSizeLessThenActual() throws ParseException {
		expectedException.expect(ParseException.class);
		expectedException.expectMessage("Decompressed data size is not equal to input size from GZIP trailer");

		ByteBuf raw = toGzip(wrapUtf8(text));
		raw.moveTail(-4);
		raw.writeInt(Integer.reverseBytes(2));

		fromGzip(raw, 11_000_000);
	}

	@Test
	public void recycleByteBufInCaseOfBadInput() throws ParseException {
		expectedException.expect(ParseException.class);
		expectedException.expectMessage("Corrupted GZIP header");

		ByteBuf badBuf = ByteBufPool.allocate(100);
		badBuf.put(new byte[]{-1, -1, -1, -1, -1, -1});

		fromGzip(badBuf, 11_000_000);
	}

	@Test
	public void testGzippedCommunicationBetweenClientServer() throws IOException {
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
				request -> request.loadBody(CHARACTERS_COUNT)
						.map(body -> {
							assertEquals("gzip", request.getHeader(CONTENT_ENCODING));
							assertEquals("gzip", request.getHeader(ACCEPT_ENCODING));

							String receivedData = body.getString(UTF_8);
							assertEquals(text, receivedData);
							return HttpResponse.ok200()
									.withBodyGzipCompression()
									.withBody(ByteBufStrings.wrapAscii(receivedData));
						}))
				.withListenPort(PORT);

		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());

		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT)
				.withHeader(ACCEPT_ENCODING, "gzip")
				.withBodyGzipCompression()
				.withBody(wrapUtf8(text));

		server.listen();

		ByteBuf body = await(client.request(request)
				.whenComplete(assertComplete(response -> assertEquals("gzip", response.getHeader(CONTENT_ENCODING))))
				.then(response -> response.loadBody(CHARACTERS_COUNT))
				.map(ByteBuf::slice)
				.whenComplete(() -> {
					server.close();
					client.stop();
				}));

		assertEquals(text, body.asString(UTF_8));
	}

	@Test
	public void testGzipInputStreamCorrectlyDecodesDataEncoded() throws IOException {
		ByteBuf encodedData = toGzip(wrapUtf8(text));
		ByteBuf decoded = decodeWithGzipInputStream(encodedData);
		assertEquals(text, decoded.asString(UTF_8));
	}

	@Test
	public void testGzipOutputStreamDataIsCorrectlyDecoded() throws IOException, ParseException {
		ByteBuf encodedData = encodeWithGzipOutputStream(wrapUtf8(text));
		ByteBuf decoded = fromGzip(encodedData, 11_000_000);
		assertEquals(text, decoded.asString(UTF_8));
	}

	private static ByteBuf encodeWithGzipOutputStream(ByteBuf raw) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
			gzip.write(raw.array(), raw.head(), raw.readRemaining());
			gzip.finish();
			byte[] bytes = baos.toByteArray();
			ByteBuf byteBuf = ByteBufPool.allocate(bytes.length);
			byteBuf.put(bytes);
			return byteBuf;
		} finally {
			raw.recycle();
		}
	}

	private static ByteBuf decodeWithGzipInputStream(ByteBuf src) throws IOException {
		try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(src.array(), src.head(), src.readRemaining()))) {
			int nRead;
			ByteBuf data = ByteBufPool.allocate(256);
			while ((nRead = gzip.read(data.array(), data.tail(), data.writeRemaining())) != -1) {
				data.moveTail(nRead);
				data = ByteBufPool.ensureWriteRemaining(data, data.readRemaining());
			}
			src.recycle();
			return data;
		}
	}

	private static String generateLargeText() {
		Random charRandom = new Random(1L);
		int charactersCount = CHARACTERS_COUNT;
		StringBuilder sb = new StringBuilder(charactersCount);
		for (int i = 0; i < charactersCount; i++) {
			int charCode = charRandom.nextInt(127);
			sb.append((char) charCode);
		}
		return sb.toString();
	}
}
