import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.dns.RemoteAsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.datakernel.config.ConfigConverters.ofInetAddress;
import static io.datakernel.di.module.Modules.combine;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * HTTP client example.
 * You can launch HttpServerExample to test this.
 */
public final class ClientExample extends Launcher {
	@Inject
	AsyncHttpClient httpClient;

	@Inject
	Eventloop eventloop;

	@Inject
	Config config;

	private String addr;

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
				.withDnsServerAddress(config.get(ofInetAddress(), "http.client.googlePublicDns"))
				.withTimeout(config.get(ofDuration(), "http.client.timeout"));
	}
	//[END REGION_1]

	//[START REGION_2]
	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(Config.create()
						.with("http.client.googlePublicDns", "8.8.8.8")
						.with("http.client.timeout", "3 seconds")
						.with("http.client.host", "http://127.0.0.1:8080"))
		);
	}
	//[END REGION_2]

	@Override
	protected void onStart() {
		addr = config.get("http.client.host");
	}

	//[START REGION_3]
	@Override
	protected void run() {
		eventloop.post(() -> {
			String msg = "Hello from client!";

			HttpRequest request = HttpRequest.post(addr).withBody(encodeAscii(msg));

			httpClient.request(request)
					.whenResult(response -> response.loadBody()
							.whenComplete((body, e) -> {
								if (e == null) {
									body = response.getBody();
									System.out.println("Server response: " + body.asString(UTF_8));
								} else {
									System.err.println("Server error: " + e);
								}
							}));
		});
	}
	//[END REGION_3]

	public static void main(String[] args) throws Exception {
		ClientExample example = new ClientExample();
		example.launch(args);
	}
}
