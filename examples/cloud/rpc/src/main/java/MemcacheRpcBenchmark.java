import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.memcache.client.MemcacheClient;
import io.datakernel.memcache.client.MemcacheClientModule;
import io.datakernel.memcache.server.MemcacheServerModule;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;

import java.io.PrintWriter;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static io.datakernel.di.module.Modules.combine;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MemcacheRpcBenchmark extends Launcher {
	private final static int MAX_REQUESTS = 10;
	private final static int WARMUP_ROUNDS = 0;
	private final static int BENCHMARK_ROUNDS = 3;
	private final static int REQUESTS_PER_TIME = 10;
	private final static boolean GENERATE_FILE = false;

	private final static int NUMBER_BUFFERS = 100;
	private final static int BUFFER_CAPACITY = 1024;
	private ByteBuf message = ByteBuf.wrapForReading("Hello world".getBytes());
	private PrintWriter resultsFile;

	@Inject
	Eventloop eventloop;

	@Provides
	Eventloop eventloop() {return Eventloop.create(); }

	@Inject
	MemcacheClient client;

	@Inject
	RpcServer server;

	@Provides
	Config config() {
		return Config.create()
				.with("memcache.buffers", Integer.toString(NUMBER_BUFFERS))
				.with("memcache.bufferCapacity", Integer.toString(BUFFER_CAPACITY))
				.with("server.listenAddresses", "localhost:8080")
				.with("client.addresses", "localhost:8080")
				.overrideWith(Config.ofProperties(System.getProperties()));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				MemcacheServerModule.create(),
				new MemcacheClientModule()
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

		profiler(this::benchmarkPut, "Put");
		profiler(this::benchmarkGet, "Get");

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
		long requestsPerSecond = (long) (MAX_REQUESTS / avgTime * 1000);
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
			resultsFile.println("    <td> -k -c " + REQUESTS_PER_TIME + "</td>");
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
		return eventloop.submit(this::benchmarkGet).get();
	}

	private long round(Supplier<Promise<Long>> function) throws Exception {
		return eventloop.submit(function).get();
	}

	private Promise<Long> benchmarkPut() {
		SettablePromise<Long> promise = new SettablePromise<>();

		long start = System.currentTimeMillis();

		Counters counters = new Counters();

		for (int i = 0; i < REQUESTS_PER_TIME; i++) {
			sendRequest(promise, counters, MAX_REQUESTS);
		}

		return promise.map($ -> System.currentTimeMillis() - start);
	}

	private Promise<Long> benchmarkGet() {
		SettablePromise<Long> promise = new SettablePromise<>();

		long start = System.currentTimeMillis();

		Counters counters = new Counters();

		for (int i = 0; i < REQUESTS_PER_TIME; i++) {
			getResponse(promise, counters, MAX_REQUESTS);
		}

		return promise.map($ -> System.currentTimeMillis() - start);
	}

	private void getResponse(SettablePromise<Long> promise, Counters counters, int limit) {
		if (counters.sentRequests == limit) {
			return;
		}
		++counters.sentRequests;

		client.get(new byte[]{(byte) counters.sentRequests}, 1000)
				.whenResult(res -> System.out.println(res.getString(UTF_8)))
				.whenComplete((res, e) -> {
					if (promise.isComplete()) {
						return;
					}

					if (e != null) {
						promise.setException(new FailedRequestException());
					}
					++counters.completedRequests;

					if (counters.completedRequests == limit) {
						promise.set(null);
						return;
					}

					getResponse(promise, counters, limit);
				});
	}

	private void sendRequest(SettablePromise<Long> promise, Counters counters, int limit) {
		if (counters.sentRequests == limit) {
			return;
		}
		++counters.sentRequests;

		client.get(new byte[]{(byte) counters.sentRequests}, 1000)
				.whenComplete((res, e) -> {
					if (promise.isComplete()) {
						return;
					}

					if (e != null) {
						promise.setException(new FailedRequestException());
						return;
					}

					++counters.completedRequests;

					if (counters.completedRequests == limit) {
						promise.set(null);
						return;
					}

					sendRequest(promise, counters, limit);
				});
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

	private static final class Counters {
		int completedRequests;
		int sentRequests;
	}

	public static void main(String[] args) throws Exception {
		Launcher benchmark = new MemcacheRpcBenchmark();
		benchmark.launch(args);
	}

	private static class FailedRequestException extends Exception {}
}
