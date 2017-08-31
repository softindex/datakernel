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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.SettableStage;
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
import java.util.concurrent.CompletionStage;

final class DnsClientConnection implements AsyncUdpSocket.EventHandler {
	@SuppressWarnings("ThrowableInstanceNeverThrown")
	private static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException();
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Map<String, Set<SettableStage<DnsQueryResult>>> resultHandlers = new HashMap<>();
	private final Eventloop eventloop;
	private final AsyncUdpSocket socket;

	private DnsClientConnection(Eventloop eventloop, AsyncUdpSocket socket) {
		this.eventloop = eventloop;
		this.socket = socket;
		this.socket.setEventHandler(this);
	}

	public static DnsClientConnection create(Eventloop eventloop, AsyncUdpSocket udpSocket) {
		return new DnsClientConnection(eventloop, udpSocket);
	}

	public CompletionStage<DnsQueryResult> resolve4(String domainName, InetSocketAddress dnsServerAddress, long timeout) {
		return resolve(domainName, dnsServerAddress, timeout, false);
	}

	public CompletionStage<DnsQueryResult> resolve6(String domainName, InetSocketAddress dnsServerAddress, long timeout) {
		return resolve(domainName, dnsServerAddress, timeout, true);
	}

	private CompletionStage<DnsQueryResult> resolve(final String domainName, InetSocketAddress dnsServerAddress, final long timeout, boolean ipv6) {
		final SettableStage<DnsQueryResult> innerStage = SettableStage.create();

		final ScheduledRunnable schedule = eventloop.schedule(eventloop.currentTimeMillis() + timeout, () -> {
			final Set<SettableStage<DnsQueryResult>> stages = resultHandlers.get(domainName);
			stages.remove(innerStage);
			if (stages.isEmpty()) resultHandlers.remove(domainName);
			innerStage.setError(TIMEOUT_EXCEPTION);
		});

		final SettableStage<DnsQueryResult> stage = SettableStage.create();
		resultHandlers.computeIfAbsent(domainName, k -> new HashSet<>()).add(innerStage);
		innerStage.whenComplete((dnsQueryResult, throwable) -> {
			if (!schedule.isCancelled() && !schedule.isComplete()) {
				schedule.cancel();
				if (throwable instanceof AsyncTimeoutException) {
					throwable = new DnsException(domainName, ResponseErrorCode.TIMED_OUT);
				}
				AsyncCallbacks.forwardTo(stage, dnsQueryResult, throwable);
			}
		});

		ByteBuf query = DnsMessage.newQuery(domainName, ipv6);
		UdpPacket queryPacket = UdpPacket.of(query, dnsServerAddress);
		socket.send(queryPacket);

		socket.read();
		return stage;
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
	public void onRead(UdpPacket packet) {
		byte[] response = packet.getBuf().array();

		try {
			DnsQueryResult dnsQueryResult = DnsMessage.getQueryResult(response);

			String domainName = dnsQueryResult.getDomainName();

			final Set<SettableStage<DnsQueryResult>> stages = resultHandlers.remove(domainName);

			if (stages != null) {
				if (dnsQueryResult.isSuccessful()) {
					for (SettableStage<DnsQueryResult> stage : stages) {
						stage.setResult(dnsQueryResult);
					}
				} else {
					final DnsException exception = dnsQueryResult.getException();
					for (SettableStage<DnsQueryResult> stage : stages) {
						stage.setError(exception);
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
	public void onSent() {
	}

	@Override
	public void onClosedWithError(Exception e) {
	}
	// endregion
}
