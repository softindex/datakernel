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

import io.datakernel.async.Stages;
import io.datakernel.eventloop.AsyncUdpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.stream.Stream;

import static io.datakernel.eventloop.Eventloop.createDatagramChannel;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.stream.Collectors.toList;

public class AsyncDnsClientConnectionTest {
	private DnsClientSocketHandler dnsClientConnection;
	private Eventloop eventloop;
	private static InetSocketAddress DNS_SERVER_ADDRESS = new InetSocketAddress("8.8.8.8", 53);
	private static long TIMEOUT = 1000L;

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Before
	public void setUp() throws Exception {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	}

	private void print(DnsQueryResult result) {
		InetAddress[] ips = result.getIps();
		System.out.print("Resolved IPs for " + result.getDomainName() + ": ");

		for (int i = 0; i < ips.length; ++i) {
			System.out.print(ips[i]);
			if (i != ips.length - 1) {
				System.out.print(", ");
			}
		}

		System.out.println(".");
	}

	@Test
	public void testResolve() throws Exception {
		DatagramChannel datagramChannel = createDatagramChannel(DatagramSocketSettings.create(), null, null);
		AsyncUdpSocketImpl udpSocket = AsyncUdpSocketImpl.create(eventloop, datagramChannel);
		dnsClientConnection = DnsClientSocketHandler.create(eventloop, udpSocket, null);
		udpSocket.setEventHandler(dnsClientConnection);
		udpSocket.register();

		Stages.collectToList(
				Stream.of("www.github.com", "www.kpi.ua", "www.google.com")
						.map(domain -> dnsClientConnection.resolve4(domain, DNS_SERVER_ADDRESS, TIMEOUT))
						.collect(toList()))
				.thenAccept(dnsQueryResults -> dnsQueryResults.forEach(this::print))
				.whenComplete(($, throwable) -> dnsClientConnection.close());

		eventloop.run();
	}
}