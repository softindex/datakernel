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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.eventloop.Eventloop.createDatagramChannel;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.net.DatagramSocketSettings.defaultDatagramSocketSettings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class NativeDnsResolverConnectionTest {
	private DnsClientHandler dnsClientConnection;
	private Eventloop eventloop;
	private static InetSocketAddress DNS_SERVER_ADDRESS = new InetSocketAddress("8.8.8.8", 53);
	private static long TIMEOUT = 1000L;
	private int answersReceived = 0;

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		eventloop = Eventloop.create();
	}

	private class DnsResolveCallback implements ResultCallback<DnsQueryResult> {
		@Override
		public void onResult(DnsQueryResult result) {
			if (result.isSuccessful()) {
				InetAddress[] ips = result.getIps();

				System.out.print("Resolved IPs for " + result.getDomainName() + ": ");

				for (int i = 0; i < ips.length; ++i) {
					System.out.print(ips[i]);
					if (i != ips.length - 1) {
						System.out.print(", ");
					}
				}

				System.out.println(".");
			} else {
				System.out.println("Resolving IPs for " + result.getDomainName() + " failed with error code: " + result.getErrorCode());
			}
			++answersReceived;
			closeConnectionIfDone();
		}

		@Override
		public void onException(Exception exception) {
			System.err.println("Exception thrown while resolving domain name. Stack trace:");
			exception.printStackTrace();
			++answersReceived;
			closeConnectionIfDone();
		}

		private void closeConnectionIfDone() {
			if (answersReceived == 3) {
				dnsClientConnection.close();
			}
		}
	}

	@Test
	public void testResolve() throws Exception {
		eventloop.postLater(new Runnable() {
			@Override
			public void run() {
				try {
					DatagramChannel datagramChannel = createDatagramChannel(defaultDatagramSocketSettings(), null, null);
					dnsClientConnection = DnsClientHandler.create(eventloop, datagramChannel);
					dnsClientConnection.register();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		eventloop.postLater(new Runnable() {
			@Override
			public void run() {
				dnsClientConnection.resolve4("www.github.com", DNS_SERVER_ADDRESS, TIMEOUT, new DnsResolveCallback());
			}
		});

		eventloop.postLater(new Runnable() {
			@Override
			public void run() {
				dnsClientConnection.resolve4("www.kpi.ua", DNS_SERVER_ADDRESS, TIMEOUT, new DnsResolveCallback());
			}
		});

		eventloop.postLater(new Runnable() {
			@Override
			public void run() {
				dnsClientConnection.resolve4("www.google.com", DNS_SERVER_ADDRESS, TIMEOUT, new DnsResolveCallback());
			}
		});

		eventloop.run();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}
}