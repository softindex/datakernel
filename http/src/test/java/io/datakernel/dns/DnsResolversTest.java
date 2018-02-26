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

package io.datakernel.dns;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static org.junit.Assert.assertEquals;

public class DnsResolversTest {
	private IAsyncDnsClient nativeDnsResolver;
	private Eventloop eventloop;
	private static final int DNS_SERVER_PORT = 53;

	private static final InetSocketAddress GOOGLE_PUBLIC_DNS = new InetSocketAddress("8.8.8.8", DNS_SERVER_PORT);
	private static final InetSocketAddress UNREACHABLE_DNS = new InetSocketAddress("8.0.8.8", DNS_SERVER_PORT);
	private static final InetSocketAddress LOCAL_DNS = new InetSocketAddress("192.168.0.1", DNS_SERVER_PORT);

	private final Logger logger = LoggerFactory.getLogger(DnsResolversTest.class);

	private static void print(InetAddress[] ips) {
		System.out.print("Resolved: ");

		for (int i = 0; i < ips.length; ++i) {
			System.out.print(ips[i]);
			if (i != ips.length - 1) {
				System.out.print(", ");
			}
		}

		System.out.println(".");
	}

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Before
	public void setUp() throws Exception {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		nativeDnsResolver = AsyncDnsClient.create(eventloop).withTimeout(3_000L).withDnsServerAddress(LOCAL_DNS);
	}

	public void testCacheInitialize(AsyncDnsClient asyncDnsClient) throws UnknownHostException {
		DnsQueryResult testResult = DnsQueryResult.successfulQuery("www.google.com",
				new InetAddress[]{InetAddress.getByName("173.194.113.210"), InetAddress.getByName("173.194.113.209")}, 10, (short) 1);
		asyncDnsClient.getCache().add(testResult);
	}

	public void testErrorCacheInitialize(AsyncDnsClient asyncDnsClient) {
		asyncDnsClient.getCache().add(new DnsException("www.google.com", DnsMessage.ResponseErrorCode.SERVER_FAILURE));
	}

	@Test
	public void testNativeDnsResolverCache() throws Exception {
		testCacheInitialize((AsyncDnsClient) nativeDnsResolver);
		CompletableFuture<InetAddress[]> future = nativeDnsResolver.resolve4("www.google.com")
				.toCompletableFuture();
		eventloop.run();

		InetAddress[] inetAddresses = future.get();
		print(inetAddresses);
	}

	@Test
	public void testNativeDnsResolverErrorCache() throws Exception {
		testErrorCacheInitialize((AsyncDnsClient) nativeDnsResolver);
		CompletableFuture<InetAddress[]> future = nativeDnsResolver.resolve4("www.google.com")
				.toCompletableFuture();
		eventloop.run();

		try {
			assertEquals(new Object(), future.get());
		} catch (ExecutionException e) {
			assertEquals(DnsMessage.ResponseErrorCode.SERVER_FAILURE, ((DnsException) e.getCause()).getErrorCode());
		}
	}

	@Test
	public void testTimeout() throws Exception {
		String domainName = "www.google.com";
		AsyncDnsClient asyncDnsClient = AsyncDnsClient.create(eventloop)
				.withTimeout(1_000L)
				.withDnsServerAddress(UNREACHABLE_DNS);

		CompletableFuture<InetAddress[]> future = asyncDnsClient.resolve4(domainName).toCompletableFuture();
		eventloop.run();
		try {
			assertEquals(new Object(), future.get());
		} catch (ExecutionException e) {
			assertEquals(DnsMessage.ResponseErrorCode.TIMED_OUT, ((DnsException) e.getCause()).getErrorCode());
		}
	}
}
