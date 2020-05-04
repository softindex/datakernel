import io.datakernel.async.callback.Callback;
import io.datakernel.common.Initializer;
import io.datakernel.common.MemSize;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.Key;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.memcache.client.MemcacheClientModule;
import io.datakernel.memcache.client.RawMemcacheClient;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.Slice;
import io.datakernel.memcache.server.MemcacheServerModule;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.service.ServiceGraphModuleSettings;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.di.module.Modules.combine;
import static java.lang.Math.min;

public class MemcacheRpcBenchmark extends Launcher {
	private final static int TOTAL_REQUESTS = 10_000_000;
	private final static int WARMUP_ROUNDS = 3;
	private final static int BENCHMARK_ROUNDS = 10;
	private final static int ACTIVE_REQUESTS_MAX = 2500;
	private final static int ACTIVE_REQUESTS_MIN = 2000;

	private final static int NUMBER_BUFFERS = 4;
	private final static MemSize BUFFER_CAPACITY = MemSize.megabytes(1);
	public static final byte[] BYTES = "Hello world".getBytes();

	private int totalRequests;
	private int warmupRounds;
	private int benchmarkRounds;
	private int activeRequestsMax;
	private int activeRequestsMin;

	@Inject
	Eventloop eventloop;

	@Provides
	Eventloop eventloop() {return Eventloop.create(); }

	@Inject
	RawMemcacheClient client;

	@Inject
	RpcServer server;

	@Inject
	Config config;

	@Provides
	Config config() {
		return Config.create()
				.with("memcache.buffers", Integer.toString(NUMBER_BUFFERS))
				.with("memcache.bufferCapacity", BUFFER_CAPACITY.format())
				.with("server.listenAddresses", "localhost:8080")
				.with("client.addresses", "localhost:8080")
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@ProvidesIntoSet
	Initializer<ServiceGraphModuleSettings> configureServiceGraph() {
		// add logical dependency so that service graph starts client only after it started the server
		return settings -> settings.addDependency(Key.of(RpcClient.class), Key.of(RpcServer.class));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger(),
				MemcacheServerModule.create(),
				MemcacheClientModule.create()
		);
	}

	@Override
	protected void onStart() {
		this.totalRequests = config.get(ofInteger(), "benchmark.totalRequests", TOTAL_REQUESTS);
		this.warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds", WARMUP_ROUNDS);
		this.benchmarkRounds = config.get(ofInteger(), "benchmark.benchmarkRounds", BENCHMARK_ROUNDS);
		this.activeRequestsMax = config.get(ofInteger(), "benchmark.activeRequestsMax", ACTIVE_REQUESTS_MAX);
		this.activeRequestsMin = config.get(ofInteger(), "benchmark.activeRequestsMin", ACTIVE_REQUESTS_MIN);
	}

	@Override
	protected void run() throws Exception {
		benchmark(this::roundPut, "Put");
		benchmark(this::roundGet, "Get");
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
		for (int i = 0; i < benchmarkRounds; i++) {
			long roundTime = round(function);
			timeAllRounds += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = roundTime;
			}

			long rps = totalRequests * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; RPS : " + rps);
		}

		double avgTime = (double) timeAllRounds / benchmarkRounds;
		long requestsPerSecond = (long) (totalRequests / avgTime * 1000);
		System.out.println("Time: " + timeAllRounds + "ms; Average time: " + avgTime + "ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Requests per second: " + requestsPerSecond);
	}

	private long round(Supplier<Promise<Long>> function) throws Exception {
		return eventloop.submit(function).get();
	}

	int sent;
	int completed;

	private Promise<Long> roundPut() {
		SettablePromise<Long> promise = new SettablePromise<>();

		Callback<Void> callback = new Callback<Void>() {
			@Override
			public void accept(Void result, @Nullable Throwable e) {
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
						doPut(this);
						sent++;
					}
				}
			}
		};
		long start = System.currentTimeMillis();
		sent = 0;
		completed = 0;

		for (int i = 0; i < min(activeRequestsMax, totalRequests); i++) {
			doPut(callback);
			sent++;
		}

		return promise.map($ -> System.currentTimeMillis() - start);
	}

	private Promise<Long> roundGet() {
		SettablePromise<Long> promise = new SettablePromise<>();

		Callback<Slice> callback = new Callback<Slice>() {
			@Override
			public void accept(Slice result, @Nullable Throwable e) {
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

				if (active <= activeRequestsMax) {
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

		for (int i = 0; i < min(activeRequestsMax, totalRequests); i++) {
			doGet(callback);
			sent++;
		}

		return promise.map($ -> System.currentTimeMillis() - start);
	}

	private void doPut(Callback<Void> callback) {
		client.put(new byte[]{(byte) sent}, new Slice(BYTES))
				.whenComplete(callback);
	}

	private void doGet(Callback<Slice> callback) {
		client.get(new byte[]{(byte) sent})
				.whenComplete(callback);
	}

	public static void main(String[] args) throws Exception {
		Launcher benchmark = new MemcacheRpcBenchmark();
		benchmark.launch(args);
	}

	private static class FailedRequestException extends Exception {}
}
