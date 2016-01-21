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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.ListenableFuture;
import io.datakernel.util.SettableFuture;
import io.datakernel.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * It is abstract class which represents non-asynchronous resolving. It resolves each domain
 * in the executor.
 */
public abstract class BlockingDnsResolver {
	static final Logger logger = LoggerFactory.getLogger(BlockingDnsResolver.class);

	protected final Map<String, ResolvedAddress> cache = new ConcurrentHashMap<>();

	protected final Map<String, ListenableFuture<InetAddress[]>> pendings = new HashMap<>();

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
	public static DnsClient getAsDnsClient(final BlockingDnsResolver resolver, final Eventloop eventloop) {
		return new DnsClient() {
			public void resolve(String domainName, final ResultCallback<InetAddress[]> callback, boolean ipv6) {
				final Eventloop.ConcurrentOperationTracker concurrentOperationTracker = eventloop.startConcurrentOperation();
				final ListenableFuture<InetAddress[]> future = ipv6 ? resolver.resolve6(domainName) : resolver.resolve4(domainName);

				future.addListener(new Runnable() {
					@Override
					public void run() {
						try {
							final InetAddress[] inetAddresses = future.get();
							eventloop.postConcurrently(new Runnable() {
								@Override
								public void run() {
									concurrentOperationTracker.complete();
									callback.onResult(inetAddresses);
								}
							});
						} catch (final InterruptedException e) {
							eventloop.postConcurrently(new Runnable() {
								@Override
								public void run() {
									concurrentOperationTracker.complete();
									callback.onException(e);
								}
							});
						} catch (final ExecutionException e) {
							eventloop.postConcurrently(new Runnable() {
								@Override
								public void run() {
									concurrentOperationTracker.complete();
									if (e.getCause() instanceof Exception)
										callback.onException((Exception) e.getCause());
								}
							});
						}
					}
				});
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
	public ListenableFuture<InetAddress[]> resolve4(String host) {
		return resolve(host, false);
	}

	/**
	 * Resolves a IP for the IPv6 addresses.
	 *
	 * @param host domain name to resolve
	 * @return future from which you can get result of resolving
	 */
	public ListenableFuture<InetAddress[]> resolve6(String host) {
		return resolve(host, true);
	}

	protected ListenableFuture<InetAddress[]> resolve(String host, boolean ipv6) {
		checkNotNull(host);

		if (Utils.isInetAddress(host)) {
			InetAddress ipAddress = Utils.forString(host);
			return SettableFuture.immediateFuture(new InetAddress[]{ipAddress});
		}

		host = host.toLowerCase();
		ResolvedAddress resolvedAddress = cache.get(host);
		if (resolvedAddress != null && !resolvedAddress.isInvalid()) {
			return SettableFuture.immediateFuture(resolvedAddress.getAllAddresses());
		}
		return resolvePending(host, ipv6);
	}

	synchronized protected ListenableFuture<InetAddress[]> resolvePending(final String host, final boolean ipv6) {
		ListenableFuture<InetAddress[]> pending = pendings.get(host);

		if (pending != null) {
			return pending;
		} else {
			final SettableFuture<InetAddress[]> future = SettableFuture.create();
			pendings.put(host, future);

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
						future.setException(new DnsException(host, DnsMessage.ResponseErrorCode.UNKNOWN));
					} else {
						future.set(resolvedAddress.getAllAddresses());
					}
				}
			});

			return future;
		}
	}

	protected abstract ResolvedAddress doResolve(String host, boolean ipv6);
}
