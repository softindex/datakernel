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
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(DatakernelRunner.class)
public class AsyncServletTest {
	@Test
	public void testEnsureRequestBody() {
		AsyncServlet servlet = request -> request.getBody().map(body -> HttpResponse.ok200().withBody(body));

		HttpRequest testRequest = HttpRequest.post("http://example.com")
				.withBodyStream(ChannelSupplier.of(
						ByteBuf.wrapForReading("Test1".getBytes(UTF_8)),
						ByteBuf.wrapForReading("Test2".getBytes(UTF_8)))
				);

		HttpResponse response = await(servlet.serve(testRequest));
		ByteBuf body = await(response.getBody());
		assertEquals("Test1Test2", body.asString(UTF_8));
	}

	@Test
	public void testEnsureRequestBodyWithException() {
		AsyncServlet servlet = request -> request.getBody().map(body -> HttpResponse.ok200().withBody(body));
		Exception exception = new Exception("TestException");

		ByteBuf byteBuf = ByteBufPool.allocate(100);
		byteBuf.put("Test1".getBytes(UTF_8));

		HttpRequest testRequest = HttpRequest.post("http://example.com")
				.withBodyStream(ChannelSuppliers.concat(
						ChannelSupplier.of(byteBuf),
						ChannelSupplier.ofException(exception)
				));

		Throwable e = awaitException(servlet.serve(testRequest));

		assertSame(exception, e);
	}
}
