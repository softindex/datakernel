package io.datakernel.dns;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.InetAddress;

import static io.datakernel.dns.DnsProtocol.ResponseErrorCode.TIMED_OUT;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static org.junit.Assert.*;

public class DnsClientWithFallbackTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private static final DnsQuery DNS_QUERY = DnsQuery.ipv4("www.test.com");

	private InetAddress[] addresses1;
	private InetAddress[] addresses2;

	private CachedAsyncDnsClient primary;
	private CachedAsyncDnsClient fallback;

	private AsyncDnsClient clientWithFallback;

	@Before
	public void setUp() throws Exception {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		primary = CachedAsyncDnsClient.create(eventloop, new MalformedDnsClient());
		fallback = CachedAsyncDnsClient.create(eventloop, new MalformedDnsClient());
		clientWithFallback = DnsClientWithFallback.create(primary, fallback);
		addresses1 = new InetAddress[]{InetAddress.getByName("173.194.113.211"), InetAddress.getByName("173.194.113.212")};
		addresses2 = new InetAddress[]{InetAddress.getByName("173.194.113.213"), InetAddress.getByName("173.194.113.214")};
	}

	@Test
	public void testResolveWithPrimary() {
		primary.getCache().add(DNS_QUERY, DnsResponse.of(DnsTransaction.of((short) 0, DNS_QUERY), DnsResourceRecord.of(addresses1, 10)));
		fallback.getCache().add(DNS_QUERY, DnsResponse.of(DnsTransaction.of((short) 0, DNS_QUERY), DnsResourceRecord.of(addresses2, 10)));

		DnsResponse response = await(clientWithFallback.resolve(DNS_QUERY));
		DnsResourceRecord record = response.getRecord();
		assertNotNull(record);
		assertArrayEquals(addresses1, record.getIps());
	}

	@Test
	public void testResolveWithFallback() {
		primary.getCache().add(DNS_QUERY, DnsResponse.ofFailure(DnsTransaction.of((short) 0, DNS_QUERY), TIMED_OUT));
		fallback.getCache().add(DNS_QUERY, DnsResponse.of(DnsTransaction.of((short) 0, DNS_QUERY), DnsResourceRecord.of(addresses2, 10)));

		DnsResponse response = await(clientWithFallback.resolve(DNS_QUERY));
		DnsResourceRecord record = response.getRecord();
		assertNotNull(record);
		assertArrayEquals(addresses2, record.getIps());
	}

	@Test
	public void testResolveWithNeither() {
		primary.getCache().add(DNS_QUERY, DnsResponse.ofFailure(DnsTransaction.of((short) 0, DNS_QUERY), TIMED_OUT));
		fallback.getCache().add(DNS_QUERY, DnsResponse.ofFailure(DnsTransaction.of((short) 0, DNS_QUERY), TIMED_OUT));

		DnsQueryException exception = awaitException(clientWithFallback.resolve(DNS_QUERY));
		assertSame(TIMED_OUT, exception.getResult().getErrorCode());
	}

	private static final class MalformedDnsClient implements AsyncDnsClient{

		@Override
		public Promise<DnsResponse> resolve(DnsQuery query) {
			throw new AssertionError("should not be called");
		}

		@Override
		public void close() {
		}
	}
}
