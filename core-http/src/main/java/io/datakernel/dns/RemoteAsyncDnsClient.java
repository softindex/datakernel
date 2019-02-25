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

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncUdpSocket;
import io.datakernel.eventloop.AsyncUdpSocket.EventHandler;
import io.datakernel.eventloop.AsyncUdpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.UdpPacket;
import io.datakernel.exception.ParseException;
import io.datakernel.inspector.AbstractInspector;
import io.datakernel.inspector.BaseInspector;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.net.DatagramSocketSettings;
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

import static io.datakernel.async.Promises.TIMEOUT_EXCEPTION;
import static io.datakernel.async.Promises.timeout;
import static io.datakernel.dns.DnsProtocol.ResponseErrorCode.TIMED_OUT;

public final class RemoteAsyncDnsClient implements AsyncDnsClient, EventHandler, EventloopJmxMBeanEx {
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
	private AsyncUdpSocketImpl.Inspector socketInspector;
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

	public RemoteAsyncDnsClient setSocketInspector(AsyncUdpSocketImpl.Inspector socketInspector) {
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

	@Override
	public MaterializedPromise<DnsResponse> resolve(DnsQuery query) {
		DnsResponse fromQuery = AsyncDnsClient.resolveFromQuery(query);
		if (fromQuery != null) {
			logger.trace("{} already contained an IP address within itself", query);
			return Promise.of(fromQuery);
		}

		if (socket == null) {
			logger.trace("Incoming query, opening UDP socket");
			try {
				DatagramChannel channel = Eventloop.createDatagramChannel(datagramSocketSettings, null, dnsServerAddress);
				AsyncUdpSocketImpl s = AsyncUdpSocketImpl.create(eventloop, channel)
						.withInspector(socketInspector);
				s.setEventHandler(this);
				s.register();
				socket = s;
			} catch (IOException e) {
				logger.error("UDP socket creation failed.", e);
				return Promise.ofException(e);
			}
		}

		logger.trace("Resolving {} with DNS server {}", query, dnsServerAddress);

		DnsTransaction transaction = DnsTransaction.of(DnsProtocol.generateTransactionId(), query);
		SettablePromise<DnsResponse> promise = new SettablePromise<>();

		transactions.put(transaction, promise);

		ByteBuf payload = DnsProtocol.createDnsQueryPayload(transaction);
		if (inspector != null) {
			inspector.onDnsQuery(query, payload);
		}
		socket.send(UdpPacket.of(payload, dnsServerAddress));
		socket.receive();
		return timeout(promise, timeout)
				.thenComposeEx((queryResult, e) -> {
					if (e == null) {
						if (inspector != null) {
							inspector.onDnsQueryResult(query, queryResult);
						}
						logger.trace("DNS query {} resolved as {}", query, queryResult.getRecord());
						return Promise.of(queryResult);
					}
					if (e == TIMEOUT_EXCEPTION) {
						logger.trace("{} timed out", query);
						e = new DnsQueryException(RemoteAsyncDnsClient.class, DnsResponse.ofFailure(transaction, TIMED_OUT));
						transactions.remove(transaction);
						closeIfDone();
					}
					if (inspector != null) {
						inspector.onDnsQueryError(query, e);
					}
					return Promise.ofException(e);
				})
				.materialize();
	}

	@Override
	public void onReceive(UdpPacket packet) {
		try {
			DnsResponse queryResult = DnsProtocol.readDnsResponse(packet.getBuf());
			SettablePromise<DnsResponse> promise = transactions.remove(queryResult.getTransaction());
			if (promise == null) {
				logger.warn("Received a DNS response that had no listener (most likely because it timed out) : {}", queryResult);
				return;
			}
			if (queryResult.isSuccessful()) {
				promise.set(queryResult);
			} else {
				promise.setException(new DnsQueryException(RemoteAsyncDnsClient.class, queryResult));
			}
			closeIfDone();
		} catch (ParseException e) {
			logger.warn("Received a UDP packet than cannot be parsed as a DNS server response.", e);
		} finally {
			packet.recycle();
		}
	}

	private void closeIfDone() {
		if (!transactions.isEmpty()) {
			return;
		}
		logger.trace("All queries complete, closing UDP socket");
		close(); // transactions is empty so no loops here
	}

	@Override
	public void onRegistered() {
	}

	@Override
	public void onSend() {
	}

	@Override
	public void onClosedWithError(Exception e) {
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
	public AsyncUdpSocketImpl.JmxInspector getSocketStats() {
		return BaseInspector.lookup(socketInspector, AsyncUdpSocketImpl.JmxInspector.class);
	}

	@JmxAttribute(name = "")
	@Nullable
	public JmxInspector getStats() {
		return BaseInspector.lookup(inspector, JmxInspector.class);
	}

}
