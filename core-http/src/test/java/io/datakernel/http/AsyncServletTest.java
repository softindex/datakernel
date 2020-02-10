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
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class AsyncServletTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testEnsureRequestBody() {
		AsyncServlet servlet = request -> request.loadBody(Integer.MAX_VALUE).map(body -> HttpResponse.ok200().withBody(body.slice()));

		HttpRequest testRequest = HttpRequest.post("http://example.com")
				.withBodyStream(ChannelSupplier.of(
						ByteBuf.wrapForReading("Test1".getBytes(UTF_8)),
						ByteBuf.wrapForReading("Test2".getBytes(UTF_8)))
				);

		HttpResponse response = await(servlet.serveAsync(testRequest));
		testRequest.recycle();
		ByteBuf body = await(response.loadBody(Integer.MAX_VALUE));

		assertEquals("Test1Test2", body.asString(UTF_8));
	}

	@Test
	public void testEnsureRequestBodyWithException() {
		AsyncServlet servlet = request -> request.loadBody(Integer.MAX_VALUE)
				.map(body -> HttpResponse.ok200().withBody(body.slice()));
		Exception exception = new Exception("TestException");

		ByteBuf byteBuf = ByteBufPool.allocate(100);
		byteBuf.put("Test1".getBytes(UTF_8));

		HttpRequest testRequest = HttpRequest.post("http://example.com")
				.withBodyStream(ChannelSuppliers.concat(
						ChannelSupplier.of(byteBuf),
						ChannelSupplier.ofException(exception)
				));

		Throwable e = awaitException(servlet.serveAsync(testRequest));

		assertSame(exception, e);
	}
}
