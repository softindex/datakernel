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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.http.HttpHeaders.ALLOW;
import static io.datakernel.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public final class TestClientMultilineHeaders {
	private static final int PORT = getFreePort();

	@Test
	public void testMultilineHeaders() throws IOException {
		AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
				request -> {
					HttpResponse response = HttpResponse.ok200();
					response.addHeader(ALLOW, "GET,\r\n HEAD");
					return Promise.of(response);
				})
				.withListenPort(PORT)
				.withAcceptOnce()
				.listen();

		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());
		String allowHeader = await(client.request(HttpRequest.get("http://127.0.0.1:" + PORT))
				.map(response -> response.getHeaderOrNull(ALLOW)));

		assertEquals("GET,   HEAD", allowHeader);
	}
}
