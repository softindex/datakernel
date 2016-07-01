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

import io.datakernel.async.ListenableResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackWithTimeout;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncUdpSocket;
import io.datakernel.eventloop.AsyncUdpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.UdpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class DnsClientHandler implements AsyncUdpSocket {
	private static final Logger logger = LoggerFactory.getLogger(DnsClientHandler.class);

	private Map<String, ListenableResultCallback<DnsQueryResult>> resultHandlers = new HashMap<>();
	private final Eventloop eventloop;
	private final AsyncUdpSocketImpl socket;

	private EventHandler upstreamEventHandler = new EventHandler() {
		@Override
		public void onRegistered() {
		}

		@Override
		public void onRead(UdpPacket packet) {
			byte[] response = packet.getBuf().array();

			try {
				DnsQueryResult dnsQueryResult = DnsMessage.getQueryResult(response);

				String domainName = dnsQueryResult.getDomainName();

				ListenableResultCallback<DnsQueryResult> callback = resultHandlers.get(domainName);

				resultHandlers.remove(domainName);

				if (callback != null) {
					if (dnsQueryResult.isSuccessful()) {
						callback.onResult(dnsQueryResult);
					} else {
						callback.onException(dnsQueryResult.getException());
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
	};

	public DnsClientHandler(Eventloop eventloop, DatagramChannel datagramChannel) {
		this.eventloop = eventloop;
		this.socket = new AsyncUdpSocketImpl(eventloop, datagramChannel);
		this.socket.setEventHandler(upstreamEventHandler);
	}

	@Override
	public void setEventHandler(EventHandler eventHandler) {
		throw new UnsupportedOperationException("can't send eventHandler for downstream");
	}

	@Override
	public void read() {
		socket.read();
	}

	@Override
	public void send(UdpPacket packet) {
		socket.send(packet);
	}

	@Override
	public void close() {
		socket.close();
	}

	public void register() {
		socket.register();
	}

	public boolean isRegistered() {
		return socket.isOpen();
	}

	private void resolve(final String domainName, InetSocketAddress dnsServerAddress, long timeout,
	                     final ResultCallback<DnsQueryResult> callback, boolean ipv6) {
		ResultCallback<DnsQueryResult> timeoutProcessingCallback = new ResultCallback<DnsQueryResult>() {
			@Override
			public void onResult(DnsQueryResult result) {
				callback.onResult(result);
			}

			@Override
			public void onException(Exception exception) {
				if (exception instanceof TimeoutException)
					resultHandlers.remove(domainName);

				callback.onException(exception);
			}
		};

		ResultCallbackWithTimeout<DnsQueryResult> callbackWithTimeout = new ResultCallbackWithTimeout<>(eventloop,
				timeoutProcessingCallback, timeout);

		if (isBeingResolved(domainName)) {
			registerCallbackForDomainName(domainName, callbackWithTimeout);
			return;
		}

		ByteBuf query = DnsMessage.newQuery(domainName, ipv6);

		ListenableResultCallback<DnsQueryResult> callbackList = new ListenableResultCallback<>();
		callbackList.addListener(callbackWithTimeout);
		resultHandlers.put(domainName, callbackList);

		UdpPacket queryPacket = new UdpPacket(query, dnsServerAddress);

		socket.send(queryPacket);

		read();
	}

	public void resolve4(String domainName, InetSocketAddress dnsServerAddress, long timeout, ResultCallback<DnsQueryResult> callback) {
		resolve(domainName, dnsServerAddress, timeout, callback, false);
	}

	public void resolve6(String domainName, InetSocketAddress dnsServerAddress, long timeout, ResultCallback<DnsQueryResult> callback) {
		resolve(domainName, dnsServerAddress, timeout, callback, true);
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

	public boolean isBeingResolved(String domainName) {
		return resultHandlers.get(domainName) != null;
	}

	private void registerCallbackForDomainName(String domainName, ResultCallback<DnsQueryResult> callback) {
		resultHandlers.get(domainName).addListener(callback);
	}
}
