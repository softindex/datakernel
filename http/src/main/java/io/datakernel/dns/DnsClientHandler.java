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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.UdpPacket;
import io.datakernel.eventloop.UdpSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class DnsClientHandler extends UdpSocketHandler {
	private Map<String, ListenableResultCallback<DnsQueryResult>> resultHandlers = new HashMap<>();

	private final Logger logger = LoggerFactory.getLogger(DnsClientHandler.class);

	/**
	 * Creates a new DNS connection
	 *
	 * @param eventloop       eventloop in which will handle this connection
	 * @param datagramChannel channel for creating this connection
	 */
	public DnsClientHandler(Eventloop eventloop, DatagramChannel datagramChannel) {
		super(eventloop, datagramChannel);
	}

	/**
	 * Receives the UDP packet with response and handles result which callback.
	 *
	 * @param packet received packet
	 */
	@Override
	protected void onRead(UdpPacket packet) {
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
			registerCallbackForDomainName(callbackWithTimeout, domainName);
			return;
		}

		ByteBuf query = DnsMessage.newQuery(domainName, ipv6);

		ListenableResultCallback<DnsQueryResult> callbackList = new ListenableResultCallback<>();
		callbackList.addListener(callbackWithTimeout);
		resultHandlers.put(domainName, callbackList);

		UdpPacket queryPacket = new UdpPacket(query, dnsServerAddress);

		send(queryPacket);
	}

	/**
	 * Sends the request for resolving IP for the IPv4 addresses and handles it with callback
	 *
	 * @param domainName       domain name for request
	 * @param dnsServerAddress address of DNS server for sending request
	 * @param timeout          time for waiting response
	 * @param callback         callback for handling result
	 */
	public void resolve4(String domainName, InetSocketAddress dnsServerAddress, long timeout, ResultCallback<DnsQueryResult> callback) {
		resolve(domainName, dnsServerAddress, timeout, callback, false);
	}

	/**
	 * Sends the request for resolving IP for the IPv6 addresses and handles it with callback
	 *
	 * @param domainName       domain name for request
	 * @param dnsServerAddress address of DNS server for sending request
	 * @param timeout          time for waiting response
	 * @param callback         callback for handling result
	 */
	public void resolve6(String domainName, InetSocketAddress dnsServerAddress, long timeout, ResultCallback<DnsQueryResult> callback) {
		resolve(domainName, dnsServerAddress, timeout, callback, true);
	}

	/**
	 * Returns the number of requests which have not response yet
	 */
	public int getNumberOfRequestsInProgress() {
		return resultHandlers.size();
	}

	/**
	 * Returns the array of strings with domains which have  been resolved, but not handled.
	 */
	public String[] getDomainNamesBeingResolved() {
		Set<String> domainNamesBeingResolved = resultHandlers.keySet();
		return domainNamesBeingResolved.toArray(new String[domainNamesBeingResolved.size()]);
	}

	public boolean allRequestsCompleted() {
		return resultHandlers.size() == 0;
	}

	/**
	 * Checks if this domain have been already resolved
	 *
	 * @param domainName string with domain name to check
	 * @return true, if it has been resolved, false else
	 */
	public boolean isBeingResolved(String domainName) {
		return resultHandlers.get(domainName) != null;
	}

	/**
	 * Sets the callback for domain name
	 *
	 * @param callback   callback for this domain
	 * @param domainName domain name for setting
	 */
	private void registerCallbackForDomainName(ResultCallback<DnsQueryResult> callback, String domainName) {
		resultHandlers.get(domainName).addListener(callback);
	}
}
