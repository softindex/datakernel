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

import com.google.common.net.InetAddresses;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.service.SimpleResultFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newHashMap;

/**
 * It is abstract class which represents non-asynchronous resolving. It resolves each domain
 * in the executor.
 */
public abstract class BlockingDnsResolver {
	static final Logger logger = LoggerFactory.getLogger(BlockingDnsResolver.class);

	protected final Map<String, ResolvedAddress> cache = newConcurrentMap();

	protected final Map<String, SimpleResultFuture<InetAddress[]>> pendings = newHashMap();

	protected long positiveKeepMillis = TimeUnit.MINUTES.toMillis(60);
	protected long negativeKeepMillis = TimeUnit.MINUTES.toMillis(10);

	protected final Executor executor;

	public BlockingDnsResolver(Executor executor) {
		this.executor = executor;
	}

	/**
	 * Returns a DNS client which resolves domains in other thread and result callback handles in thread of eventloop.
	 *
	 * @param resolver  DNS resolver for client
	 * @param eventloop eventloop in which it will calls callback
	 * @return new DNS Client
	 */
	public static DnsClient getAsDnsClient(final BlockingDnsResolver resolver, final NioEventloop eventloop) {
		return new DnsClient() {
			public void resolve(String domainName, final ResultCallback<InetAddress[]> callback, boolean ipv6) {
				final Eventloop.ConcurrentOperationTracker concurrentOperationTracker = eventloop.startConcurrentOperation();

				SimpleResultFuture<InetAddress[]> simpleResultFuture = new SimpleResultFuture<InetAddress[]>() {
					@Override
					protected void doOnResult(final InetAddress[] item) {
						eventloop.postConcurrently(new Runnable() {
							@Override
							public void run() {
								concurrentOperationTracker.complete();
								callback.onResult(item);
							}
						});
					}

					@Override
					protected void doOnError(Exception e) {
						concurrentOperationTracker.complete();
						callback.onException(e);
					}
				};

				if (ipv6) {
					resolver.resolve6(domainName, simpleResultFuture);
				} else {
					resolver.resolve4(domainName, simpleResultFuture);
				}
			}

			@Override
			public void resolve4(String domainName, ResultCallback<InetAddress[]> callback) {
				resolve(domainName, callback, false);
			}

			@Override
			public void resolve6(String domainName, final ResultCallback<InetAddress[]> callback) {
				resolve(domainName, callback, true);
			}
		};
	}

	public void setPositiveKeepMillis(long positiveKeepMillis) {
		this.positiveKeepMillis = positiveKeepMillis;
	}

	public void setNegativeKeepMillis(long negativeKeepMillis) {
		this.negativeKeepMillis = negativeKeepMillis;
	}

	protected static class ResolvedAddress {
		private final InetAddress[] addresses;
		private final long updateTimestamp;
		private volatile int counter;

		ResolvedAddress(InetAddress[] address, long ttl) {
			this.addresses = address;
			this.updateTimestamp = System.currentTimeMillis() + ttl;
		}

		boolean isInvalid() {
			return (System.currentTimeMillis() > updateTimestamp);
		}

		InetAddress getAddress() {
			if (addresses == null)
				return null;
			InetAddress address = addresses[counter];
			counter = (counter + 1) % addresses.length;
			return address;
		}

		InetAddress[] getAllAddresses() {
			return addresses;
		}

		public long getUpdateTimestamp() {
			return updateTimestamp;
		}
	}

	/**
	 * Resolves a IP for the IPv4 addresses.
	 *
	 * @param host domain name to resolve
	 * @return future from which you can get result of resolving
	 */
	public void resolve4(String host, SimpleResultFuture<InetAddress[]> simpleResultFuture) {
		resolve(host, false, simpleResultFuture);
	}

	/**
	 * Resolves a IP for the IPv6 addresses.
	 *
	 * @param host domain name to resolve
	 * @return future from which you can get result of resolving
	 */
	public void resolve6(String host, SimpleResultFuture<InetAddress[]> simpleResultFuture) {
		resolve(host, true, simpleResultFuture);
	}

	protected void resolve(String host, boolean ipv6, SimpleResultFuture<InetAddress[]> simpleResultFuture) {
		checkNotNull(host);

		if (InetAddresses.isInetAddress(host)) {
			InetAddress ipAddress = InetAddresses.forString(host);
			simpleResultFuture.onResult(new InetAddress[]{ipAddress});
			return;
		}

		host = host.toLowerCase();
		ResolvedAddress resolvedAddress = cache.get(host);
		if (resolvedAddress != null && !resolvedAddress.isInvalid()) {
			simpleResultFuture.onResult(resolvedAddress.getAllAddresses());
			return;
		}
		resolvePending(host, ipv6, simpleResultFuture);
	}

	synchronized protected void resolvePending(final String host, final boolean ipv6, final SimpleResultFuture<InetAddress[]> simpleResultFuture) {
		SimpleResultFuture<InetAddress[]> pending = pendings.get(host);

		if (pending != null) {
			try {
				pending.onResult(pending.get());
			} catch (Exception e) {
				throw new RuntimeException();
			}
		} else {
			pendings.put(host, simpleResultFuture);

			executor.execute(new Runnable() {
				@Override
				public void run() {
					final ResolvedAddress resolvedAddress = doResolve(host, ipv6);
					synchronized (BlockingDnsResolver.this) {
						cache.put(host, resolvedAddress);
						pendings.remove(host);
					}

					InetAddress[] inetAddresses = resolvedAddress.getAllAddresses();

					if (inetAddresses == null) {
						simpleResultFuture.onError(new DnsException(host, DnsMessage.ResponseErrorCode.UNKNOWN));
					} else {
						simpleResultFuture.onResult(resolvedAddress.getAllAddresses());
					}
				}
			});
		}
	}

	protected abstract ResolvedAddress doResolve(String host, boolean ipv6);
}
