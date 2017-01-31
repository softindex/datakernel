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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.*;
import io.datakernel.exception.AsyncTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class DnsClientHandler implements AsyncUdpSocket {
	private static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException();
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Map<String, Set<ResultCallback<DnsQueryResult>>> resultHandlers = new HashMap<>();
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

				final Set<ResultCallback<DnsQueryResult>> callbacks = resultHandlers.remove(domainName);

				if (callbacks != null) {
					if (dnsQueryResult.isSuccessful()) {
						for (ResultCallback<DnsQueryResult> callback : callbacks) {
							callback.setResult(dnsQueryResult);
						}
					} else {
						final DnsException exception = dnsQueryResult.getException();
						for (ResultCallback<DnsQueryResult> callback : callbacks) {
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
		public void onSent() {
		}

		@Override
		public void onClosedWithError(Exception e) {
		}
	};

	private DnsClientHandler(Eventloop eventloop, DatagramChannel datagramChannel) {
		this.eventloop = eventloop;
		this.socket = AsyncUdpSocketImpl.create(eventloop, datagramChannel);
		this.socket.setEventHandler(upstreamEventHandler);
	}

	public static DnsClientHandler create(Eventloop eventloop, DatagramChannel datagramChannel) {
		return new DnsClientHandler(eventloop, datagramChannel);
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

	private void resolve(final String domainName, InetSocketAddress dnsServerAddress, final long timeout,
	                     final ResultCallback<DnsQueryResult> callback, boolean ipv6) {
		final ResultCallback<DnsQueryResult> callbackWithTimeout = new ResultCallback<DnsQueryResult>() {
			private final ScheduledRunnable timeouter = eventloop.schedule(eventloop.currentTimeMillis() + timeout, new ScheduledRunnable() {
				@Override
				public void run() {
					final Set<ResultCallback<DnsQueryResult>> callbacks = resultHandlers.get(domainName);
					callbacks.remove(getThisCallback());
					if (callbacks.isEmpty()) resultHandlers.remove(domainName);
					setException(TIMEOUT_EXCEPTION);
				}
			});

			private ResultCallback<DnsQueryResult> getThisCallback() {
				return this;
			}

			@Override
			protected void onResult(DnsQueryResult result) {
				if (!timeouter.isCancelled() && !timeouter.isComplete()) {
					timeouter.cancel();
					callback.setResult(result);
				}
			}

			@Override
			protected void onException(Exception e) {
				if (!timeouter.isCancelled() && !timeouter.isComplete()) {
					timeouter.cancel();
					callback.setException(e);
				}
			}
		};

		Set<ResultCallback<DnsQueryResult>> callbacks = resultHandlers.get(domainName);
		if (callbacks == null) {
			callbacks = new HashSet<>();
			resultHandlers.put(domainName, callbacks);
		}
		callbacks.add(callbackWithTimeout);

		ByteBuf query = DnsMessage.newQuery(domainName, ipv6);

		UdpPacket queryPacket = UdpPacket.of(query, dnsServerAddress);

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

}
