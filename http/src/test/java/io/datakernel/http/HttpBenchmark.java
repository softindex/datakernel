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

import com.google.common.net.InetAddresses;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.SimpleCompletionFuture;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.service.NioEventloopRunner;

import java.util.concurrent.CountDownLatch;

import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static io.datakernel.util.ByteBufStrings.decodeUTF8;

public class HttpBenchmark extends Benchmark {
	private NioEventloop eventloop;
	private NioEventloopRunner eventloopRunner;
	private HttpClientAsync httpClient;
	private final int port;

	private int complete = 0;
	private int fails = 0;

	public HttpBenchmark(int port) throws Exception {
		super("Http Requests/Responses", 3, 100, 1_000);
		this.port = port;
	}

	@Override
	protected void setUp() throws Exception {
		eventloop = new NioEventloop();
		eventloopRunner = new NioEventloopRunner(eventloop);
		AsyncHttpServer server = HelloWorldServer.helloWorldServer(eventloop, port);
		eventloopRunner.addNioServers(server);
		httpClient = new HttpClientImpl(eventloop, new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS,
				3_000L, InetAddresses.forString("127.0.0.1")));
		SimpleCompletionFuture callback = new SimpleCompletionFuture();
		eventloopRunner.startFuture(callback);
		callback.await();
	}

	@Override
	protected void beforeRound() throws Exception {
		complete = 0;
		fails = 0;
	}

	@Override
	protected void round() throws Exception {
		final String uri = "http://127.0.0.1:" + port + "/";
		final CountDownLatch latch = new CountDownLatch(1);
		for (int i = 0; i < operations; i++) {
			eventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {
					httpClient.getHttpResultAsync(HttpRequest.get(uri), 3000, new ResultCallback<HttpResponse>() {
						@Override
						public void onResult(HttpResponse result) {
							complete++;
							if (!result.getBody().equalsTo(HelloWorldServer.HELLO_WORLD))
								throw new RuntimeException("Received result: " + decodeUTF8(result.getBody()));
							checkEnd();
						}

						@Override
						public void onException(Exception exception) {
							fails++;
							checkEnd();
						}

						private void checkEnd() {
							if ((complete + fails) >= operations) {
								latch.countDown();
							}
						}
					});
				}
			});
		}
		latch.await();
	}

	@Override
	protected void afterRound() throws Exception {
		System.out.println("Round end with complete: " + complete + "; fails: " + fails);
	}

	@Override
	protected void tearDown() throws Exception {
		SimpleCompletionFuture callback = new SimpleCompletionFuture();
		eventloopRunner.stopFuture(callback);
		callback.await();

	}

	public static void main(String[] args) throws Exception {
		Benchmark benchmark = new HttpBenchmark(47777);
		benchmark.run();
	}

}
