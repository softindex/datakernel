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

package io.global.common;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class CipherTransformerTest {

	@Test
	public void test() {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		List<ByteBuf> data = Arrays.asList(
				ByteBuf.wrapForReading("hello world, this is some lines of text to be encrypted, yaay!".getBytes(UTF_8)),
				ByteBuf.wrapForReading("here is also some pretty random text that, however, looks very distinctly for the human eye".getBytes(UTF_8)),
				ByteBuf.wrapForReading("third one is the worst one, we're played ourselves, hurray".getBytes(UTF_8))
		);

		SimKey key = SimKey.generate();
		byte[] nonce = CryptoUtils.nonceFromString("test.txt");
		long pos = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE >>> 1);

		ChannelSupplier.ofIterable(data)
				.transformWith(CipherTransformer.create(key, nonce, pos))
				.toList()
				.whenComplete(assertComplete(enc -> enc.forEach(System.out::println)))
				.thenCompose(enc -> ChannelSupplier.ofIterable(enc)
						.transformWith(CipherTransformer.create(key, nonce, pos))
						.toList())
				.whenComplete(assertComplete(dec -> {
					dec.forEach(System.out::println);
					assertEquals(data, dec);
				}));

		eventloop.run();
	}
}
