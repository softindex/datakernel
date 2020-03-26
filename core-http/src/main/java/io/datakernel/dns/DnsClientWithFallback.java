package io.datakernel.dns;

import io.datakernel.promise.Promise;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public final class DnsClientWithFallback implements AsyncDnsClient {
	private static final Logger logger = getLogger(DnsClientWithFallback.class);

	private final AsyncDnsClient primaryDnsClient;
	private final AsyncDnsClient fallbackDnsClient;

	private DnsClientWithFallback(AsyncDnsClient primaryDnsClient, AsyncDnsClient fallbackDnsClient) {
		this.primaryDnsClient = primaryDnsClient;
		this.fallbackDnsClient = fallbackDnsClient;
	}

	public static DnsClientWithFallback create(AsyncDnsClient primaryDnsClient, AsyncDnsClient fallbackDnsClient) {
		return new DnsClientWithFallback(primaryDnsClient, fallbackDnsClient);
	}

	@Override
	public Promise<DnsResponse> resolve(DnsQuery query) {
		return primaryDnsClient.resolve(query)
				.thenEx((response, e) -> {
					if (e == null) {
						logger.trace("Successfully resolved DNS query ({}) with primary client", query);
						return Promise.of(response);
					} else {
						logger.trace("Could not resolve DNS query ({}) with primary client, trying to resolve with fallback client", query, e);
						return fallbackDnsClient.resolve(query)
								.whenComplete((result, e1) -> {
									if (e1 == null){
										logger.trace("Successfully resolved DNS query ({}) with fallback client", query);
									} else {
										logger.trace("Could not resolve DNS query ({}) with fallback client", query, e);
									}
								});
					}
				});
	}

	@Override
	public void close() {
		primaryDnsClient.close();
		fallbackDnsClient.close();
	}
}
