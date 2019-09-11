import io.datakernel.async.callback.Callback;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import io.datakernel.service.ServiceGraphModule;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.di.module.Modules.combine;
import static java.lang.Math.min;

public class HttpServerWorkloadBenchmark extends Launcher {
	private final static int KEEP_ALIVE = 30;
	private final static int TOTAL_REQUESTS = 1_000_000;
	private final static int WARMUP_ROUNDS = 3;
	private final static int BENCHMARK_ROUNDS = 5;
	private final static int ACTIVE_REQUESTS_MAX = 300;
	private final static int ACTIVE_REQUESTS_MIN = 200;

	private String address;
	private int totalRequests;
	private int warmupRounds;
	private int measureRounds;
	private int activeRequestsMax;
	private int activeRequestsMin;

	@Provides
	@Named("server")
	Eventloop serverEventloop() { return Eventloop.create(); }

	@Provides
	@Named("client")
	Eventloop clientEventloop() { return Eventloop.create(); }

	@Inject
	@Named("server")
	Eventloop serverEventloop;

	@Inject
	@Named("client")
	Eventloop clientEventloop;

	@Inject
	Config config;

	@Inject
	AsyncHttpServer server;

	@Inject
	AsyncHttpClient client;

	@Provides
	AsyncHttpServer server() {
		return AsyncHttpServer.create(serverEventloop,
				request ->
						HttpResponse.ok200().withPlainText("Response!!"))
				.withListenAddresses(config.get(ofList(ofInetSocketAddress()), "address"));
	}

	@Provides
	AsyncHttpClient client() {
		return AsyncHttpClient.create(clientEventloop)
				.withKeepAliveTimeout(Duration.ofSeconds(config.get(ofInteger(),
						"client.keepAlive", KEEP_ALIVE)));
	}

	@Provides
	Config config() {
		return Config.create()
				.with("address", "0.0.0.0:9001")
				.with("client.address", "http://127.0.0.1:9001/")
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {})
		);
	}

	@Override
	protected void onStart() throws Exception {
		this.address = config.get("client.address");
		this.totalRequests = config.get(ofInteger(), "benchmark.totalRequests", TOTAL_REQUESTS);
		this.warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds", WARMUP_ROUNDS);
		this.measureRounds = config.get(ofInteger(), "benchmark.measureRounds", BENCHMARK_ROUNDS);
		this.activeRequestsMax = config.get(ofInteger(), "benchmark.activeRequestsMax", ACTIVE_REQUESTS_MAX);
		this.activeRequestsMin = config.get(ofInteger(), "benchmark.activeRequestsMin", ACTIVE_REQUESTS_MIN);
		super.onStart();
	}

	@Override
	protected void run() throws Exception {
		benchmark(this::roundGet, "GET Request");
	}

	private void benchmark(Supplier<Promise<Long>> function, String nameBenchmark) throws Exception {
		long timeAllRounds = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < warmupRounds; i++) {
			long roundTime = round(function);
			long rps = totalRequests * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; RPS : " + rps);
		}

		System.out.println("Start benchmarking " + nameBenchmark);
		for (int i = 0; i < measureRounds; i++) {
			long roundTime = round(function);
			timeAllRounds += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = roundTime;
			}

			long rps = totalRequests * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + " ms; RPS : " + rps);
		}

		double avgTime = (double) timeAllRounds / measureRounds;
		long requestsPerSecond = (long) (totalRequests / avgTime * 1000);
		System.out.println("Time: " + timeAllRounds + "ms; Average time: " + avgTime + " ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Requests per second: " + requestsPerSecond);

	}

	private long round(Supplier<Promise<Long>> function) throws Exception {
		return clientEventloop.submit(function).get();
	}

	int sent;
	int completed;

	private Promise<Long> roundGet() {
		SettablePromise<Long> promise = new SettablePromise<>();

		Callback<HttpResponse> callback = new Callback<HttpResponse>() {
			@Override
			public void accept(HttpResponse result, @Nullable Throwable e) {
				completed++;
				int active = sent - completed;

				if (e != null) {
					promise.setException(new FailedRequestException());
					return;
				}

				if (completed == totalRequests) {
					promise.set(null);
					return;
				}

				if (active <= activeRequestsMin) {
					for (int i = 0; i < min(activeRequestsMax - active, totalRequests - sent); i++) {
						doGet(this);
						sent++;
					}
				}
			}
		};
		sent = 0;
		completed = 0;
		long start = System.currentTimeMillis();

		for (int i = 0; i < min(activeRequestsMin, totalRequests); i++) {
			doGet(callback);
			sent++;
		}

		return promise.map($ -> System.currentTimeMillis() - start);
	}

	private void doGet(Callback<HttpResponse> callback) {
		client.request(HttpRequest.get(address)).whenComplete(callback);
	}

	public static void main(String[] args) throws Exception {
		Launcher benchmark = new HttpServerWorkloadBenchmark();
		benchmark.launch(args);
	}

	private static class FailedRequestException extends Exception {}
}
