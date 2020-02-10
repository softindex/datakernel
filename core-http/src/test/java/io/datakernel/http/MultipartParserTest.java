/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class MultipartParserTest {
	private static final String BOUNDARY = "--test-boundary-123";
	private static final String CRLF = "\r\n";

	private static final String DATA = BOUNDARY + CRLF +
			"Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"" + CRLF +
			"Content-Type: text/plain" + CRLF +
			CRLF +
			"This is some bytes of data to be extracted from the multipart form" + CRLF +
			"Also here we had a wild CRLF se\r\nquence appear" +
			CRLF + BOUNDARY + CRLF +
			"Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"" + CRLF +
			"Content-Type: text/plain" + CRLF +
			"Test-Extra-Header: one" + CRLF +
			"Test-Extra-Header-2: two" + CRLF +
			CRLF +
			"\nAnd the second " +
			CRLF + BOUNDARY + CRLF +
			CRLF +
			"line, huh\n" +
			CRLF + BOUNDARY + "--" + CRLF;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() {
		List<ByteBuf> split = new ArrayList<>();
		int i = 0;
		while (i < DATA.length() / 5 - 1) {
			split.add(ByteBuf.wrapForReading(DATA.substring(i * 5, ++i * 5).getBytes(UTF_8)));
		}
		if (DATA.length() != (i *= 5)) {
			split.add(ByteBuf.wrapForReading(DATA.substring(i).getBytes(UTF_8)));
		}

		List<Map<String, String>> headers = new ArrayList<>();

		String res = await(BinaryChannelSupplier.of(ChannelSupplier.ofIterable(split))
				.parseStream(MultipartParser.create(BOUNDARY.substring(2)))
				.toCollector(mapping(frame -> {
					if (frame.isData()) {
						return frame.getData().asString(UTF_8);
					}
					assertFalse(frame.getHeaders().isEmpty());
					headers.add(frame.getHeaders());
					return "";
				}, joining())));

		assertEquals("This is some bytes of data to be extracted from the multipart form\r\n" +
				"Also here we had a wild CRLF se\r\nquence appear\n" +
				"And the second line, huh\n", res);
		assertEquals(asList(
				map("content-disposition", "form-data; name=\"file\"; filename=\"test.txt\"",
						"content-type", "text/plain"),
				map("content-disposition", "form-data; name=\"file\"; filename=\"test.txt\"",
						"content-type", "text/plain",
						"test-extra-header", "one",
						"test-extra-header-2", "two")
		), headers);
	}

	@Test
	public void testSplitOnlyLastPart() {
		// last boundary
		ByteBuf buf = ByteBufStrings.wrapUtf8(BOUNDARY + "--" + CRLF);
		MultipartParser parser = MultipartParser.create(BOUNDARY.substring(2));

		await(parser.split(ChannelSupplier.of(buf), new MultipartParser.MultipartDataHandler() {
			@Override
			public Promise<? extends ChannelConsumer<ByteBuf>> handleField(String fieldName) {
				return Promise.ofException(new AssertionError());
			}

			@Override
			public Promise<? extends ChannelConsumer<ByteBuf>> handleFile(String fieldName, String fileName) {
				return Promise.ofException(new AssertionError());
			}
		}));
	}
}
