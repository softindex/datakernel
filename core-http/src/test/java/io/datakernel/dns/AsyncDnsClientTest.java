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

package io.datakernel.dns;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.inspector.AbstractInspector;
import io.datakernel.common.ref.RefInt;
import io.datakernel.dns.RemoteAsyncDnsClient.Inspector;
import io.datakernel.dns.protocol.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import org.junit.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.datakernel.dns.protocol.DnsProtocol.ResponseErrorCode.*;
import static io.datakernel.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Ignore
public final class AsyncDnsClientTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private CachedAsyncDnsClient cachedDnsClient;
	private static final int DNS_SERVER_PORT = 53;

	private static final InetSocketAddress UNREACHABLE_DNS = new InetSocketAddress("8.0.8.8", DNS_SERVER_PORT);
	private static final InetSocketAddress LOCAL_DNS = new InetSocketAddress("192.168.0.1", DNS_SERVER_PORT);

	@Before
	public void setUp() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		cachedDnsClient = CachedAsyncDnsClient.create(eventloop, RemoteAsyncDnsClient.create(eventloop).withDnsServerAddress(LOCAL_DNS));
	}

	@Test
	public void testCachedDnsClient() throws Exception {
		DnsQuery query = DnsQuery.ipv4("www.google.com");
		InetAddress[] ips = {InetAddress.getByName("173.194.113.210"), InetAddress.getByName("173.194.113.209")};

		cachedDnsClient.getCache().add(query, DnsResponse.of(DnsTransaction.of((short) 0, query), DnsResourceRecord.of(ips, 10)));
		DnsResponse result = await(cachedDnsClient.resolve4("www.google.com"));

		assertNotNull(result.getRecord());
		System.out.println(Arrays.stream(result.getRecord().getIps())
				.map(InetAddress::toString)
				.collect(joining(", ", "Resolved: ", ".")));
	}

	@Test
	public void testCachedDnsClientError() {
		DnsQuery query = DnsQuery.ipv4("www.google.com");

		cachedDnsClient.getCache().add(query, DnsResponse.ofFailure(DnsTransaction.of((short) 0, query), SERVER_FAILURE));

		DnsQueryException e = awaitException(cachedDnsClient.resolve4("www.google.com"));
		assertEquals(SERVER_FAILURE, e.getResult().getErrorCode());
	}

	@Test
	public void testDnsClient() {
		AsyncDnsClient dnsClient = RemoteAsyncDnsClient.create(Eventloop.getCurrentEventloop());

		await(Promises.toList(Stream.of("www.google.com", "www.github.com", "www.kpi.ua")
				.map(dnsClient::resolve4)));
	}

	@Test
	public void testDnsClientTimeout() {
		RemoteAsyncDnsClient dnsClient = RemoteAsyncDnsClient.create(Eventloop.getCurrentEventloop())
				.withTimeout(Duration.ofMillis(20))
				.withDnsServerAddress(UNREACHABLE_DNS);

		DnsQueryException e = awaitException(dnsClient.resolve4("www.google.com"));
		assertEquals(TIMED_OUT, e.getResult().getErrorCode());
	}

	@Test
	public void testDnsNameError() {
		AsyncDnsClient dnsClient = RemoteAsyncDnsClient.create(Eventloop.getCurrentEventloop());

		DnsQueryException e = awaitException(dnsClient.resolve4("example.ensure-such-top-domain-it-will-never-exist"));
		assertEquals(NAME_ERROR, e.getResult().getErrorCode());
	}

	@Test
	@Ignore
	public void testAdaptedClientsInMultipleThreads() {
		int threadCount = 10;
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		InspectorGadget inspector = new InspectorGadget();
		CachedAsyncDnsClient primaryCachedDnsClient = CachedAsyncDnsClient.create(
				eventloop,
				RemoteAsyncDnsClient.create(eventloop)
						.withInspector(inspector),
				DnsCache.create(eventloop)
		);

		RefInt index = new RefInt(-1);
		IntStream.range(0, threadCount)
				.mapToObj($ -> (Runnable) () -> {
					eventloop.startExternalTask();
					Eventloop subloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

					AsyncDnsClient cachedClient = primaryCachedDnsClient.adaptToAnotherEventloop(subloop);

					Promises.toList(
							Stream.generate(() -> null)
									.flatMap($2 -> Stream.of("www.google.com", "www.github.com", "www.kpi.ua"))
									.limit(100)
									.map(cachedClient::resolve4))
							.thenEx(($2, e) -> {
								if (e instanceof DnsQueryException) {
									if (((DnsQueryException) e).getResult().getErrorCode() == TIMED_OUT) {
										System.out.println("TIMED_OUT");
										return Promise.complete();
									}
								}
								return Promise.of(null, e);
							})
							.whenComplete(assertComplete());

					try {
						subloop.run();
					} catch (Throwable e) {
						eventloop.recordFatalError(e, subloop);
					} finally {
						eventloop.completeExternalTask();
					}
				})
				.forEach(runnable -> new Thread(runnable, "test thread #" + index.inc()).start());
		await();

		System.out.println("Real requests per query:");
		inspector.getRequestCounts().forEach((k, v) -> System.out.println(v + " of " + k));
		inspector.getRequestCounts().forEach((k, v) -> assertEquals(1, v.intValue()));
	}

	private static class InspectorGadget extends AbstractInspector<Inspector> implements Inspector {
		private Map<DnsQuery, Integer> requestCounts = new ConcurrentHashMap<>();

		public Map<DnsQuery, Integer> getRequestCounts() {
			return requestCounts;
		}

		@Override
		public void onDnsQuery(DnsQuery query, ByteBuf payload) {
			requestCounts.merge(query, 1, Integer::sum);
		}

		@Override
		public void onDnsQueryResult(DnsQuery query, DnsResponse result) {

		}

		@Override
		public void onDnsQueryError(DnsQuery query, Throwable e) {

		}

		@Override
		public void onDnsQueryExpiration(DnsQuery query) {

		}
	}
}
