import io.datakernel.async.Callback;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.service.ServiceGraphModule;
import org.jetbrains.annotations.Nullable;

import java.io.PrintWriter;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofList;
import static io.datakernel.di.module.Modules.combine;
import static java.lang.Math.min;

/**
 * @author is Alex Syrotenko (@pantokrator)
 * Created on 07.08.19.
 */
public class HttpServerWorkloadBenchmark extends Launcher {
	private final static int TOTAL_REQUESTS = 1_000_000;
	private final static int WARMUP_ROUNDS = 1;
	private final static int BENCHMARK_ROUNDS = 5;
	private final static int ACTIVE_REQUESTS_MAX = 500;
	private final static int ACTIVE_REQUESTS_MIN = 300;
	private final static boolean GENERATE_FILE = false;
	private PrintWriter resultsFile;

	@Provides
	@Named("server")
	Eventloop serverEventloop() { return Eventloop.create(); }

	@Provides
	@Named("client")
	Eventloop clientEventloop() { return Eventloop.create(); }

	@Provides
	Config config() {
		return Config.create()
				.with("address", "0.0.0.0:9001")
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}

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
				AsyncServlet.of(req -> HttpResponse.ok200().withPlainText("Response!!")))
				.withListenAddresses(config.get(ofList(ofInetSocketAddress()), "address"));
	}

	@Provides
	AsyncHttpClient client() {
		return AsyncHttpClient.create(clientEventloop)
				.withKeepAliveTimeout(Duration.ofSeconds(30));
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
	protected void run() throws Exception {
		if (GENERATE_FILE) {
			resultsFile = new PrintWriter("benchmarkResult" + Timestamp.from(Instant.now()), "UTF-8");
			fillHeaderFile(resultsFile);
		}

		if (WARMUP_ROUNDS > 0) {
			System.out.println("Start warming up cache");
			warmUp();
		}

		profiler(this::benchmark, "GET Request");

		if (GENERATE_FILE) {
			resultsFile.close();
		}
	}

	private void profiler(Supplier<Promise<Long>> function, String nameBenchmark) throws Exception {
		long timeAllRounds = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Start benchmarking " + nameBenchmark);
		for (int i = 0; i < BENCHMARK_ROUNDS; i++) {
			long roundTime = round(function);
			timeAllRounds += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = roundTime;
			}

			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms");
		}

		double avgTime = (double) timeAllRounds / BENCHMARK_ROUNDS;
		long requestsPerSecond = (long) (TOTAL_REQUESTS / avgTime * 1000);
		System.out.println("Time: " + timeAllRounds + "ms; Average time: " + avgTime + "ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Requests per second: " + requestsPerSecond);

		if (GENERATE_FILE) {
			fillFooterFile(avgTime, bestTime, worstTime, requestsPerSecond);
		}
	}

	private void fillFooterFile(double avgTime,
			long bestTime,
			long worstTime,
			long requestsPerSecond) {
		if (GENERATE_FILE) {
			resultsFile.println("    <td> -k -c " + ACTIVE_REQUESTS_MAX + "</td>");
			resultsFile.println("    <td>" + avgTime + "</td>");
			resultsFile.println("    <td>" + bestTime + "</td>");
			resultsFile.println("    <td>" + worstTime + "</td>");
			resultsFile.println("    <td>" + requestsPerSecond + "</td>");
			resultsFile.println("  </tr>");
			resultsFile.println("</table>");
		}
	}

	private void warmUp() throws Exception {
		for (int i = 0; i < WARMUP_ROUNDS; i++) {
			round();
		}
	}

	private long round() throws Exception {
		return clientEventloop.submit(this::benchmark).get();
	}

	private long round(Supplier<Promise<Long>> function) throws Exception {
		return clientEventloop.submit(function).get();
	}

	int sent;
	int completed;

	private Promise<Long> benchmark() {
		SettablePromise<Long> promise = new SettablePromise<>();

		Callback<HttpResponse> callback = new Callback<HttpResponse>() {
			@Override
			public void accept(HttpResponse result, @Nullable Throwable e) {
				completed++;
				int active = sent - completed;

				if (e != null) {
					e.printStackTrace();
					promise.setException(new FailedRequestException());
					return;
				}

				if (completed == TOTAL_REQUESTS) {
					promise.set(null);
					return;
				}

				if (active <= ACTIVE_REQUESTS_MIN) {
					for (int i = 0; i < min(ACTIVE_REQUESTS_MAX - active, TOTAL_REQUESTS - sent); i++) {
						sendRequest(this);
						sent++;
					}
				}
			}
		};
		long start = System.currentTimeMillis();
		sent = 0;
		completed = 0;

		for (int i = 0; i < min(ACTIVE_REQUESTS_MIN, TOTAL_REQUESTS); i++) {
			sendRequest(callback);
			sent++;
		}

		return promise.map($ -> System.currentTimeMillis() - start);
	}

	private void sendRequest(Callback<HttpResponse> callback) {
		client.request(HttpRequest.get("http://127.0.0.1:9001/")).whenComplete(callback);
	}

	private void fillHeaderFile(PrintWriter resultsFile) {
		resultsFile.println("<table>");
		resultsFile.println("  <tr>");
		resultsFile.println("    <th>ApacheBench parameters</th>");
		resultsFile.println("    <th>Time</th>");
		resultsFile.println("    <th>Average time</th>");
		resultsFile.println("    <th>Best time</th>");
		resultsFile.println("    <th>Worst time</th>");
		resultsFile.println("    <th>Requests per second</th>");
		resultsFile.println("  </tr>");
	}

	public static void main(String[] args) throws Exception {
		Launcher benchmark = new HttpServerWorkloadBenchmark();
		benchmark.launch(args);
	}

	private static class FailedRequestException extends Exception {}
}
