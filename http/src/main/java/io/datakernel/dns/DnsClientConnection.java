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

import io.datakernel.async.Callback;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.dns.DnsMessage.ResponseErrorCode;
import io.datakernel.eventloop.AsyncUdpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.eventloop.UdpPacket;
import io.datakernel.exception.AsyncTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class DnsClientConnection implements AsyncUdpSocket.EventHandler {
	@SuppressWarnings("ThrowableInstanceNeverThrown")
	private static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException();
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Map<String, Set<Callback<DnsQueryResult>>> resultHandlers = new HashMap<>();
	private final Eventloop eventloop;
	private final AsyncUdpSocket socket;

	private AsyncDnsClient.Inspector inspector;

	private DnsClientConnection(Eventloop eventloop, AsyncUdpSocket socket, AsyncDnsClient.Inspector inspector) {
		this.eventloop = eventloop;
		this.socket = socket;
		this.socket.setEventHandler(this);
		this.inspector = inspector;
	}

	public static DnsClientConnection create(Eventloop eventloop, AsyncUdpSocket udpSocket,
	                                         AsyncDnsClient.Inspector inspector) {
		return new DnsClientConnection(eventloop, udpSocket, inspector);
	}

	public void resolve4(String domainName, InetSocketAddress dnsServerAddress, long timeout, Callback<DnsQueryResult> callback) {
		resolve(domainName, dnsServerAddress, timeout, callback, false);
	}

	Stage<DnsQueryResult> resolve4(String domainName, InetSocketAddress dnsServerAddress, long timeout) {
		SettableStage<DnsQueryResult> result = SettableStage.create();
		resolve(domainName, dnsServerAddress, timeout, result, false);
		return result;
	}

	public void resolve6(String domainName, InetSocketAddress dnsServerAddress, long timeout, Callback<DnsQueryResult> callback) {
		resolve(domainName, dnsServerAddress, timeout, callback, true);
	}

	Stage<DnsQueryResult> resolve6(String domainName, InetSocketAddress dnsServerAddress, long timeout) {
		SettableStage<DnsQueryResult> result = SettableStage.create();
		resolve(domainName, dnsServerAddress, timeout, result, true);
		return result;
	}

	private void resolve(String domainName, InetSocketAddress dnsServerAddress, long timeout,
	                     Callback<DnsQueryResult> callback, boolean ipv6) {
		Callback<DnsQueryResult> callbackWithTimeout = new Callback<DnsQueryResult>() {
			private final ScheduledRunnable timeouter = eventloop.delay(timeout, () -> {
				Set<Callback<DnsQueryResult>> callbacks = resultHandlers.get(domainName);
				callbacks.remove(getThisCallback());
				if (callbacks.isEmpty()) resultHandlers.remove(domainName);
				setException(TIMEOUT_EXCEPTION);
			});

			private Callback<DnsQueryResult> getThisCallback() {
				return this;
			}

			@Override
			public void set(DnsQueryResult result) {
				if (!timeouter.isCancelled() && !timeouter.isComplete()) {
					timeouter.cancel();
					if (inspector != null) inspector.onDnsQueryResult(domainName, result);
					callback.set(result);
				}
			}

			@Override
			public void setException(Throwable e) {
				if (!timeouter.isCancelled() && !timeouter.isComplete()) {
					if (e instanceof AsyncTimeoutException) {
						e = new DnsException(domainName, ResponseErrorCode.TIMED_OUT);
					}
					timeouter.cancel();
					if (inspector != null) inspector.onDnsQueryError(domainName, e);
					callback.setException(e);
				}
			}
		};

		Set<Callback<DnsQueryResult>> callbacks = resultHandlers.get(domainName);
		if (callbacks == null) {
			callbacks = new HashSet<>();
			resultHandlers.put(domainName, callbacks);
		}
		callbacks.add(callbackWithTimeout);
		ByteBuf query = DnsMessage.newQuery(domainName, ipv6);
		if (inspector != null) inspector.onDnsQuery(domainName, query);
		UdpPacket queryPacket = UdpPacket.of(query, dnsServerAddress);
		socket.send(queryPacket);

		socket.receive();
	}

	public void close() {
		socket.close();
	}

	public int getNumberOfRequestsInProgress() {
		return resultHandlers.size();
	}

	public String[] getDomainNamesBeingResolved() {
		Set<String> domainNamesBeingResolved = resultHandlers.keySet();
		return domainNamesBeingResolved.toArray(new String[domainNamesBeingResolved.size()]);
	}

	public boolean allRequestsCompleted() {
		return resultHandlers.size() == 0;
	}

	// region AsyncUdpSocket.EventHandler implementation
	@Override
	public void onRegistered() {
	}

	@Override
	public void onReceive(UdpPacket packet) {
		byte[] response = packet.getBuf().array();

		try {
			DnsQueryResult dnsQueryResult = DnsMessage.getQueryResult(response);

			String domainName = dnsQueryResult.getDomainName();

			Set<Callback<DnsQueryResult>> callbacks = resultHandlers.remove(domainName);

			if (callbacks != null) {
				if (dnsQueryResult.isSuccessful()) {
					for (Callback<DnsQueryResult> callback : callbacks) {
						callback.set(dnsQueryResult);
					}
				} else {
					DnsException exception = dnsQueryResult.getException();
					for (Callback<DnsQueryResult> callback : callbacks) {
						callback.setException(exception);
					}
				}
			}
		} catch (DnsResponseParseException e) {
			logger.info("Received packet cannot be parsed as DNS server response.", e);
		} finally {
			packet.recycle();
		}
	}

	@Override
	public void onSend() {
	}

	@Override
	public void onClosedWithError(Exception e) {
	}
	// endregion
}
