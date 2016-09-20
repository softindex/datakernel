package io.datakernel;

import io.datakernel.async.ResultCallback;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.http.MediaTypes.*;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class ClientStressTest {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final String PATH_TO_URLS = "./src/test/resources/urls.txt";

	private Eventloop eventloop = Eventloop.create();
	private ExecutorService executor = newCachedThreadPool();
	private Random random = new Random();
	private Iterator<String> urls = getUrls().iterator();

	private AsyncHttpServlet servlet = new AsyncHttpServlet() {
		@Override
		public void serveAsync(HttpRequest request, Callback callback) throws ParseException {
			test();
			callback.onResult(HttpResponse.ok200());
		}
	};
	private AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet).withListenPort(1234);

	private final SSLContext context = SSLContext.getDefault();

	private AsyncHttpClient client = AsyncHttpClient.create(eventloop,
			NativeDnsResolver.create(eventloop).withDnsServerAddress(inetAddress("8.8.8.8")))
			.withSslEnabled(context, executor);

	private ClientStressTest() throws Exception {}

	private void doTest() throws IOException {
		server.listen();
		eventloop.run();
	}

	private void test() {
		int delay = random.nextInt(10000);
		final String url = urls.next();
		if (url != null) {
			eventloop.schedule(eventloop.currentTimeMillis() + delay, new Runnable() {
				@Override
				public void run() {
					logger.info("sending request to: {}", url);
					client.send(formRequest(url, random.nextBoolean()), 5000, new ResultCallback<HttpResponse>() {
						@Override
						public void onResult(HttpResponse result) {
							logger.info("url: {}, succeed", url);
						}

						@Override
						public void onException(Exception e) {
							logger.error("url: {}, failed", url, e);
						}
					});
					test();
				}
			});
		} else {
			server.close();
		}
	}

	private List<String> getUrls() throws IOException {
		return StringUtils.splitToList('\n', new String(Files.readAllBytes(Paths.get(PATH_TO_URLS))));
	}

	private HttpRequest formRequest(String url, boolean keepAlive) {
		HttpRequest request = HttpRequest.get(url);
		request.addHeader(CACHE_CONTROL, "max-age=0");
		request.addHeader(ACCEPT_ENCODING, "gzip, deflate, sdch");
		request.addHeader(ACCEPT_LANGUAGE, "en-US,en;q=0.8");
		request.addHeader(USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36");
		request.setAccept(AcceptMediaType.of(HTML),
				AcceptMediaType.of(XHTML_APP),
				AcceptMediaType.of(XML_APP, 90),
				AcceptMediaType.of(WEBP),
				AcceptMediaType.of(ANY, 80));
		if (keepAlive) {
			request.addHeader(CONNECTION, "keep-alive");
		}
		return request;
	}

	public static void main(String[] args) throws Exception {
		new ClientStressTest().doTest();
	}
}
