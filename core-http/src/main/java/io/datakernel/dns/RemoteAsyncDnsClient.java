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
import io.datakernel.common.inspector.BaseInspector;
import io.datakernel.common.parse.ParseException;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventStats;
import io.datakernel.eventloop.jmx.EventloopJmxMBeanEx;
import io.datakernel.eventloop.net.DatagramSocketSettings;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.net.AsyncUdpSocket;
import io.datakernel.net.AsyncUdpSocketNio;
import io.datakernel.net.UdpPacket;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.promise.Promises.TIMEOUT_EXCEPTION;
import static io.datakernel.promise.Promises.timeout;

/**
 * Implementation of {@link AsyncDnsClient} that asynchronously
 * connects to some <i>real</i> DNS server and gets the response from it.
 */
public final class RemoteAsyncDnsClient implements AsyncDnsClient, EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(RemoteAsyncDnsClient.class);

	public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(3);
	private static final int DNS_SERVER_PORT = 53;
	public static final InetSocketAddress GOOGLE_PUBLIC_DNS = new InetSocketAddress("8.8.8.8", DNS_SERVER_PORT);
	public static final InetSocketAddress LOCAL_DNS = new InetSocketAddress("192.168.0.1", DNS_SERVER_PORT);

	private final Eventloop eventloop;
	private final Map<DnsTransaction, SettablePromise<DnsResponse>> transactions = new HashMap<>();

	private DatagramSocketSettings datagramSocketSettings = DatagramSocketSettings.create();
	private InetSocketAddress dnsServerAddress = GOOGLE_PUBLIC_DNS;
	private Duration timeout = DEFAULT_TIMEOUT;

	@Nullable
	private AsyncUdpSocket socket;

	@Nullable
	private AsyncUdpSocketNio.Inspector socketInspector;
	@Nullable
	private Inspector inspector;

	// region creators
	private RemoteAsyncDnsClient(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	public static RemoteAsyncDnsClient create(Eventloop eventloop) {
		return new RemoteAsyncDnsClient(eventloop);
	}

	public RemoteAsyncDnsClient withDatagramSocketSetting(DatagramSocketSettings setting) {
		this.datagramSocketSettings = setting;
		return this;
	}

	public RemoteAsyncDnsClient withTimeout(Duration timeout) {
		this.timeout = timeout;
		return this;
	}

	public RemoteAsyncDnsClient withDnsServerAddress(InetSocketAddress address) {
		this.dnsServerAddress = address;
		return this;
	}

	public RemoteAsyncDnsClient withDnsServerAddress(InetAddress address) {
		this.dnsServerAddress = new InetSocketAddress(address, DNS_SERVER_PORT);
		return this;
	}

	public RemoteAsyncDnsClient withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	public RemoteAsyncDnsClient setSocketInspector(AsyncUdpSocketNio.Inspector socketInspector) {
		this.socketInspector = socketInspector;
		return this;
	}

	// endregion

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void close() {
		if (socket == null) {
			return;
		}
		socket.close();
		socket = null;
		transactions.values().forEach(s -> s.setException(TIMEOUT_EXCEPTION));
	}

	private Promise<AsyncUdpSocket> getSocket() {
		AsyncUdpSocket socket = this.socket;
		if (socket != null) {
			return Promise.of(socket);
		}
		try {
			logger.trace("Incoming query, opening UDP socket");
			DatagramChannel channel = Eventloop.createDatagramChannel(datagramSocketSettings, null, dnsServerAddress);
			return AsyncUdpSocketNio.connect(eventloop, channel)
					.map(s -> this.socket = s.withInspector(socketInspector));
		} catch (IOException e) {
			logger.error("UDP socket creation failed.", e);
			return Promise.ofException(e);
		}
	}

	@Override
	public Promise<DnsResponse> resolve(DnsQuery query) {
		DnsResponse fromQuery = AsyncDnsClient.resolveFromQuery(query);
		if (fromQuery != null) {
			logger.trace("{} already contained an IP address within itself", query);
			return Promise.of(fromQuery);
		}
		// ignore the result because soon or later it will be sent and just completed
		// here we use that transactions map because it easily could go completely out of order and we should be ok with that
		return getSocket()
				.then(socket -> {
					logger.trace("Resolving {} with DNS server {}", query, dnsServerAddress);

					DnsTransaction transaction = DnsTransaction.of(DnsProtocol.generateTransactionId(), query);
					SettablePromise<DnsResponse> promise = new SettablePromise<>();

					transactions.put(transaction, promise);

					ByteBuf payload = DnsProtocol.createDnsQueryPayload(transaction);
					if (inspector != null) {
						inspector.onDnsQuery(query, payload);
					}

					// ignore the result because soon or later it will be sent and just completed
					socket.send(UdpPacket.of(payload, dnsServerAddress));

					// here we use that transactions map because it easily could go completely out of order and we should be ok with that
					socket.receive()
							.whenResult(packet -> {
								try {
									DnsResponse queryResult = DnsProtocol.readDnsResponse(packet.getBuf());
									SettablePromise<DnsResponse> cb = transactions.remove(queryResult.getTransaction());
									if (cb == null) {
										logger.warn("Received a DNS response that had no listener (most likely because it timed out) : {}", queryResult);
										return;
									}
									if (queryResult.isSuccessful()) {
										cb.set(queryResult);
									} else {
										cb.setException(new DnsQueryException(RemoteAsyncDnsClient.class, queryResult));
									}
									closeIfDone();
								} catch (ParseException e) {
									logger.warn("Received a UDP packet than cannot be parsed as a DNS server response.", e);
								} finally {
									packet.recycle();
								}
							});

					return timeout(timeout, promise)
							.thenEx((queryResult, e) -> {
								if (e == null) {
									if (inspector != null) {
										inspector.onDnsQueryResult(query, queryResult);
									}
									logger.trace("DNS query {} resolved as {}", query, queryResult.getRecord());
									return Promise.of(queryResult);
								}
								if (e == TIMEOUT_EXCEPTION) {
									logger.trace("{} timed out", query);
									e = new DnsQueryException(RemoteAsyncDnsClient.class, DnsResponse.ofFailure(transaction, DnsProtocol.ResponseErrorCode.TIMED_OUT));
									transactions.remove(transaction);
									closeIfDone();
								}
								if (inspector != null) {
									inspector.onDnsQueryError(query, e);
								}
								return Promise.ofException(e);
							});
				});
	}

	private void closeIfDone() {
		if (!transactions.isEmpty()) {
			return;
		}
		logger.trace("All queries complete, closing UDP socket");
		close(); // transactions is empty so no loops here
	}

	// region JMX
	public interface Inspector extends BaseInspector<Inspector> {
		void onDnsQuery(DnsQuery query, ByteBuf payload);

		void onDnsQueryResult(DnsQuery query, DnsResponse result);

		void onDnsQueryError(DnsQuery query, Throwable e);
	}

	public static class JmxInspector extends AbstractInspector<Inspector> implements Inspector {
		private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		private final EventStats queries = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats failedQueries = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats expirations = EventStats.create(SMOOTHING_WINDOW);

		@Override
		public void onDnsQuery(DnsQuery query, ByteBuf payload) {
			queries.recordEvent();
		}

		@Override
		public void onDnsQueryResult(DnsQuery query, DnsResponse result) {
			if (!result.isSuccessful()) {
				failedQueries.recordEvent();
			}
		}

		@Override
		public void onDnsQueryError(DnsQuery query, Throwable e) {
			failedQueries.recordEvent();
		}

		@JmxAttribute
		public EventStats getQueries() {
			return queries;
		}

		@JmxAttribute
		public EventStats getFailedQueries() {
			return failedQueries;
		}

		@JmxAttribute
		public EventStats getExpirations() {
			return expirations;
		}
	}
	// endregion

	@JmxAttribute
	@Nullable
	public AsyncUdpSocketNio.JmxInspector getSocketStats() {
		return BaseInspector.lookup(socketInspector, AsyncUdpSocketNio.JmxInspector.class);
	}

	@JmxAttribute(name = "")
	@Nullable
	public JmxInspector getStats() {
		return BaseInspector.lookup(inspector, JmxInspector.class);
	}
}
