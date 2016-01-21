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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.Eventloop.ConcurrentOperationTracker;
import io.datakernel.time.SettableCurrentTimeProvider;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static org.junit.Assert.*;

public class DnsResolversTest {
	private DnsClient nativeDnsResolver;
	private DnsClient simpleDnsResolver;
	private Eventloop eventloop;
	private static final int DNS_SERVER_PORT = 53;

	private static final InetSocketAddress GOOGLE_PUBLIC_DNS = new InetSocketAddress("8.8.8.8", DNS_SERVER_PORT);
	private static final InetSocketAddress UNREACHABLE_DNS = new InetSocketAddress("8.0.8.8", DNS_SERVER_PORT);
	private static final InetSocketAddress LOCAL_DNS = new InetSocketAddress("192.168.0.1", DNS_SERVER_PORT);

	private final Logger logger = LoggerFactory.getLogger(DnsResolversTest.class);

	private class ConcurrentDnsResolveCallback implements ResultCallback<InetAddress[]> {
		private final DnsResolveCallback callback;
		private final ConcurrentOperationTracker localEventloopConcurrentOperationTracker;
		private final ConcurrentOperationsCounter counter;

		public ConcurrentDnsResolveCallback(DnsResolveCallback callback,
		                                    ConcurrentOperationTracker localEventloopConcurrentOperationTracker,
		                                    ConcurrentOperationsCounter counter) {
			this.callback = callback;
			this.localEventloopConcurrentOperationTracker = localEventloopConcurrentOperationTracker;
			this.counter = counter;
		}

		@Override
		public void onResult(InetAddress[] result) {
			callback.onResult(result);
			localEventloopConcurrentOperationTracker.complete();
			counter.completeOperation();
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
			localEventloopConcurrentOperationTracker.complete();
			counter.completeOperation();
		}
	}

	private class DnsResolveCallback implements ResultCallback<InetAddress[]> {
		private InetAddress[] result = null;
		private Exception exception = null;

		@Override
		public void onResult(@Nullable InetAddress[] ips) {
			this.result = ips;

			if (ips == null) {
				return;
			}

			for (int i = 0; i < ips.length; ++i) {
				System.out.print(ips[i]);
				if (i != ips.length - 1) {
					System.out.print(", ");
				}
			}

			System.out.println(".");
		}

		@Override
		public void onException(Exception e) {
			this.exception = e;
			System.out.println("Resolving IP addresses for host failed. Stack trace: ");
			e.printStackTrace();
		}

		public InetAddress[] getResult() {
			return result;
		}

		public Exception getException() {
			return exception;
		}
	}

	private final class ConcurrentOperationsCounter {
		private final int concurrentOperations;
		private final ConcurrentOperationTracker concurrentOperationTracker;
		private AtomicInteger operationsCompleted = new AtomicInteger(0);

		private ConcurrentOperationsCounter(int concurrentOperations, ConcurrentOperationTracker concurrentOperationTracker) {
			this.concurrentOperationTracker = concurrentOperationTracker;
			this.concurrentOperations = concurrentOperations;
		}

		public void completeOperation() {
			if (operationsCompleted.incrementAndGet() == concurrentOperations) {
				concurrentOperationTracker.complete();
			}
		}
	}

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		eventloop = new Eventloop();
		Executor executor = Executors.newFixedThreadPool(5);

		nativeDnsResolver = new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS,
				3_000L, LOCAL_DNS);
		simpleDnsResolver = BlockingDnsResolver.getAsDnsClient(new SimpleDnsResolver(executor), eventloop);
	}

	@Ignore
	@Test
	public void testResolversWithCorrectDomainNames() throws Exception {
		DnsResolveCallback simpleResult1 = new DnsResolveCallback();
		DnsResolveCallback nativeResult1 = new DnsResolveCallback();
		simpleDnsResolver.resolve4("www.google.com", simpleResult1);
		nativeDnsResolver.resolve4("www.google.com", nativeResult1);

		DnsResolveCallback simpleResult2 = new DnsResolveCallback();
		DnsResolveCallback nativeResult2 = new DnsResolveCallback();
		simpleDnsResolver.resolve4("www.stackoverflow.com", simpleResult2);
		nativeDnsResolver.resolve4("www.stackoverflow.com", nativeResult2);

		DnsResolveCallback simpleResult3 = new DnsResolveCallback();
		DnsResolveCallback nativeResult3 = new DnsResolveCallback();
		simpleDnsResolver.resolve4("microsoft.com", simpleResult3);
		nativeDnsResolver.resolve4("microsoft.com", nativeResult3);

		eventloop.run();

		assertEquals(newHashSet(simpleResult1.result), newHashSet(nativeResult1.result));
		assertEquals(newHashSet(simpleResult2.result), newHashSet(nativeResult2.result));
		assertEquals(newHashSet(simpleResult3.result), newHashSet(nativeResult3.result));

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@SafeVarargs
	private final <V> Set<V> newHashSet(V... result) {
		Set<V> set = new HashSet<>();
		set.addAll(Arrays.asList(result));
		return set;
	}

	@Ignore
	@Test
	public void testResolve6() throws Exception {
		DnsResolveCallback simpleDnsResolverCallback = new DnsResolveCallback();
		simpleDnsResolver.resolve6("www.google.com", simpleDnsResolverCallback);

		simpleDnsResolver.resolve6("www.flickr.com", new DnsResolveCallback());
		simpleDnsResolver.resolve6("www.kpi.ua", new DnsResolveCallback());

		DnsResolveCallback nativeDnsResolverCallback = new DnsResolveCallback();
		nativeDnsResolver.resolve6("www.google.com", nativeDnsResolverCallback);

		nativeDnsResolver.resolve6("www.stackoverflow.com", new DnsResolveCallback());
		nativeDnsResolver.resolve6("www.oracle.com", new DnsResolveCallback());

		eventloop.run();

		InetAddress simpleResult[] = simpleDnsResolverCallback.result;
		InetAddress nativeResult[] = nativeDnsResolverCallback.result;

		assertNotNull(simpleResult);
		assertNotNull(nativeResult);

		Set<InetAddress> simpleResultList = newHashSet(simpleResult);
		Set<InetAddress> nativeResultList = newHashSet(nativeResult);

		assertEquals(simpleResultList, nativeResultList);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Ignore
	@Test
	public void testResolversWithIncorrectDomainNames() throws Exception {
		DnsResolveCallback simpleDnsResolverCallback = new DnsResolveCallback();
		simpleDnsResolver.resolve4("rfwaufnau", simpleDnsResolverCallback);

		DnsResolveCallback nativeDnsResolverCallback = new DnsResolveCallback();
		nativeDnsResolver.resolve4("fsafa", nativeDnsResolverCallback);

		eventloop.run();

		assertTrue(nativeDnsResolverCallback.exception instanceof DnsException);
		assertTrue(simpleDnsResolverCallback.exception instanceof DnsException);

		assertNull(simpleDnsResolverCallback.result);
		assertNull(nativeDnsResolverCallback.result);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Ignore
	@Test
	public void testConcurrentNativeResolver() throws InterruptedException {
		Eventloop primaryEventloop = this.eventloop;
		final NativeDnsResolver nativeDnsResolver = (NativeDnsResolver) this.nativeDnsResolver;

		ConcurrentOperationsCounter counter = new ConcurrentOperationsCounter(10, primaryEventloop.startConcurrentOperation());

		resolveInAnotherThreadWithDelay(nativeDnsResolver, "www.google.com", 0, counter);
		resolveInAnotherThreadWithDelay(nativeDnsResolver, "www.google.com", 0, counter);
		resolveInAnotherThreadWithDelay(nativeDnsResolver, "www.google.com", 0, counter);
		resolveInAnotherThreadWithDelay(nativeDnsResolver, "www.google.com", 500, counter);
		resolveInAnotherThreadWithDelay(nativeDnsResolver, "www.google.com", 1000, counter);
		resolveInAnotherThreadWithDelay(nativeDnsResolver, "www.google.com", 1000, counter);
		resolveInAnotherThreadWithDelay(nativeDnsResolver, "www.google.com", 1500, counter);
		resolveInAnotherThreadWithDelay(nativeDnsResolver, "www.stackoverflow.com", 1500, counter);
		resolveInAnotherThreadWithDelay(nativeDnsResolver, "www.yahoo.com", 1500, counter);
		resolveInAnotherThreadWithDelay(nativeDnsResolver, "www.google.com", 1500, counter);

		primaryEventloop.run();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private void resolveInAnotherThreadWithDelay(final NativeDnsResolver nativeDnsResolver, final String domainName,
	                                             final int delayMillis, final ConcurrentOperationsCounter counter) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(delayMillis);
				} catch (InterruptedException e) {
					logger.warn("Thread interrupted.", e);
				}
				Eventloop callerEventloop = new Eventloop();
				ConcurrentOperationTracker concurrentOperationTracker = callerEventloop.startConcurrentOperation();
				ConcurrentDnsResolveCallback callback = new ConcurrentDnsResolveCallback(new DnsResolveCallback(),
						concurrentOperationTracker, counter);
				logger.info("Attempting to resolve host {} from thread {}.", domainName, Thread.currentThread().getName());
				DnsClient dnsClient = nativeDnsResolver.getDnsClientForAnotherEventloop(callerEventloop);
				dnsClient.resolve4(domainName, callback);
				callerEventloop.run();
				logger.info("Thread {} execution finished.", Thread.currentThread().getName());
			}
		}).start();
	}

	public void testCacheInitialize(NativeDnsResolver nativeDnsResolver) throws UnknownHostException {
		DnsQueryResult testResult = DnsQueryResult.successfulQuery("www.google.com",
				new InetAddress[]{InetAddress.getByName("173.194.113.210"), InetAddress.getByName("173.194.113.209")}, 10, (short) 1);
		nativeDnsResolver.getCache().add(testResult);
	}

	public void testErrorCacheInitialize(NativeDnsResolver nativeDnsResolver) {
		nativeDnsResolver.getCache().add(new DnsException("www.google.com", DnsMessage.ResponseErrorCode.SERVER_FAILURE));
	}

	@Test
	public void testNativeDnsResolverCache() throws Exception {
		DnsResolveCallback callback = new DnsResolveCallback();
		testCacheInitialize((NativeDnsResolver) nativeDnsResolver);
		nativeDnsResolver.resolve4("www.google.com", callback);

		eventloop.run();

		assertNotNull(callback.result);
		assertNull(callback.exception);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testNativeDnsResolverErrorCache() throws Exception {
		DnsResolveCallback callback = new DnsResolveCallback();
		testErrorCacheInitialize((NativeDnsResolver) nativeDnsResolver);
		nativeDnsResolver.resolve4("www.google.com", callback);

		eventloop.run();

		assertTrue(callback.exception instanceof DnsException);
		assertNull(callback.result);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testNativeDnsResolverCache2() throws Exception {
		String domainName = "www.google.com";
		DnsQueryResult testResult = DnsQueryResult.successfulQuery(domainName,
				new InetAddress[]{InetAddress.getByName("173.194.113.210"), InetAddress.getByName("173.194.113.209")},
				3, (short) 1);

		SettableCurrentTimeProvider timeProvider = new SettableCurrentTimeProvider(0);
		Eventloop eventloopWithTimeProvider = new Eventloop(timeProvider);
		NativeDnsResolver nativeResolver = new NativeDnsResolver(eventloopWithTimeProvider, DEFAULT_DATAGRAM_SOCKET_SETTINGS,
				3_000L, GOOGLE_PUBLIC_DNS);
		DnsCache cache = nativeResolver.getCache();

		timeProvider.setTime(0);
		eventloopWithTimeProvider.refreshTimestampAndGet();
		cache.add(testResult);

		timeProvider.setTime(1500);
		eventloopWithTimeProvider.refreshTimestampAndGet();
		nativeResolver.resolve4(domainName, new DnsResolveCallback());

		timeProvider.setTime(3500);
		eventloopWithTimeProvider.refreshTimestampAndGet();
		DnsResolveCallback callback = new DnsResolveCallback();
		nativeResolver.resolve4(domainName, callback);
		eventloopWithTimeProvider.run();
		cache.add(testResult);

		timeProvider.setTime(70000);
		eventloopWithTimeProvider.refreshTimestampAndGet();
		nativeResolver.resolve4(domainName, new DnsResolveCallback());
		eventloopWithTimeProvider.run();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testTimeout() throws Exception {
		String domainName = "www.google.com";
		NativeDnsResolver nativeDnsResolver = new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS,
				3_000L, UNREACHABLE_DNS);
		nativeDnsResolver.resolve4(domainName, new DnsResolveCallback());
		eventloop.run();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}
