import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.dns.RemoteAsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpMessage;
import io.datakernel.http.HttpRequest;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.datakernel.config.ConfigConverters.ofInetAddress;
import static io.datakernel.di.module.Modules.combine;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * HTTP client example.
 * You can launch HttpServerExample to test this.
 */
public final class HttpClientExample extends Launcher {
	@Inject
	AsyncHttpClient httpClient;

	@Inject
	Eventloop eventloop;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	//[START REGION_1]
	@Provides
	AsyncHttpClient client(Eventloop eventloop, AsyncDnsClient dnsClient) {
		return AsyncHttpClient.create(eventloop)
				.withDnsClient(dnsClient);
	}

	@Provides
	AsyncDnsClient dnsClient(Eventloop eventloop, Config config) {
		return RemoteAsyncDnsClient.create(eventloop)
				.withDnsServerAddress(config.get(ofInetAddress(), "dns.address"))
				.withTimeout(config.get(ofDuration(), "dns.timeout"));
	}
	//[END REGION_1]

	//[START REGION_2]
	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create().printEffectiveConfig()
		);
	}

	@Provides
	Config config() {
		return Config.create()
				.with("dns.address", "8.8.8.8")
				.with("dns.timeout", "5 seconds")
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}
	//[END REGION_2]

	//[START REGION_3]
	@Override
	protected void run() throws ExecutionException, InterruptedException {
		String url = args.length != 0 ? args[0] : "http://127.0.0.1:8080/";
		System.out.println("HTTP request: " + url);
		CompletableFuture<String> future = eventloop.submit(() ->
				httpClient.request(HttpRequest.get(url))
						.then(HttpMessage::loadBody)
						.map(body -> body.getString(UTF_8))
		);
		System.out.println("HTTP response: " + future.get());
	}
	//[END REGION_3]

	public static void main(String[] args) throws Exception {
		HttpClientExample example = new HttpClientExample();
		example.launch(args);
	}
}
