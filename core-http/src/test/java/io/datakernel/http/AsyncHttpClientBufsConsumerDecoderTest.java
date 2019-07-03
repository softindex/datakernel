package io.datakernel.http;

import io.datakernel.dns.RemoteAsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.util.StringFormatUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;

import static io.datakernel.async.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static junit.framework.TestCase.assertNotNull;

@Ignore
public class AsyncHttpClientBufsConsumerDecoderTest {
	@ClassRule
	public static EventloopRule eventloopRule = new EventloopRule();

	private static final String URL = "https://o7planning.org/ru/10399/jsoup-java-html-parser-tutorial";
	private static AsyncHttpClient client;

	@BeforeClass
	public static void init() throws NoSuchAlgorithmException, IOException {
		RemoteAsyncDnsClient dnsClient = RemoteAsyncDnsClient.create(Eventloop.getCurrentEventloop())
				.withDnsServerAddress(InetAddress.getByName("8.8.8.8"))
				.withTimeout(StringFormatUtils.parseDuration("5 seconds"));
		client = AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.withSslEnabled(SSLContext.getDefault(), newSingleThreadExecutor())
				.withDnsClient(dnsClient);
	}

	@Test
	public void run() {
		String result = await(client.request(HttpRequest.get(URL))
				.then(HttpMessage::loadBody)
				.map(body -> body.getString(UTF_8)));
		assertNotNull(result);
	}
}
