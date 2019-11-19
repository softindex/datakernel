import io.datakernel.async.callback.Callback;
import io.datakernel.common.Initializer;
import io.datakernel.common.MemSize;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.annotation.Qualifier;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.service.ServiceGraphModuleSettings;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static java.lang.Math.min;

@SuppressWarnings("WeakerAccess")
public class RpcBenchmark extends Launcher {
	private final static int TOTAL_REQUESTS = 10000000;
	private final static int WARMUP_ROUNDS = 3;
	private final static int BENCHMARK_ROUNDS = 10;
	private final static int SERVICE_PORT = 25565;
	private final static int ACTIVE_REQUESTS_MIN = 10000;
	private final static int ACTIVE_REQUESTS_MAX = 10000;

	@Inject
	RpcClient rpcClient;

	@Inject
	RpcServer rpcServer;

	@Inject
	@Qualifier("client")
	Eventloop eventloop;

	@Inject
	Config config;

	@Provides
	@Qualifier("client")
	Eventloop eventloopClient() {
		return Eventloop.create()
				.withFatalErrorHandler(rethrowOnAnyError());
	}

	@Provides
	@Qualifier("server")
	Eventloop eventloopServer(@Qualifier("client") Eventloop clientEventloop, Config config) {
		return config.get(ofBoolean(), "multithreaded", true) ?
				Eventloop.create()
						.withFatalErrorHandler(rethrowOnAnyError()) :
				clientEventloop;
	}

	@Provides
	public RpcClient rpcClient(@Qualifier("client") Eventloop eventloop, Config config) {
		return RpcClient.create(eventloop)
				.withStreamProtocol(
						config.get(ofMemSize(), "rpc.defaultPacketSize", MemSize.kilobytes(256)),
						ChannelSerializer.MAX_SIZE_1,
						config.get(ofBoolean(), "rpc.compression", false))
				.withMessageTypes(Integer.class)
				.withStrategy(server(new InetSocketAddress(config.get(ofInteger(), "rpc.server.port"))));
	}

	@Provides
	public RpcServer rpcServer(@Qualifier("server") Eventloop eventloop, Config config) {
		return RpcServer.create(eventloop)
				.withStreamProtocol(
						config.get(ofMemSize(), "rpc.defaultPacketSize", MemSize.kilobytes(256)),
						ChannelSerializer.MAX_SIZE_1,
						config.get(ofBoolean(), "rpc.compression", false))
				.withListenPort(config.get(ofInteger(), "rpc.server.port"))
				.withMessageTypes(Integer.class)
				.withHandler(Integer.class, Integer.class, req -> Promise.of(req * 2));

	}

	@ProvidesIntoSet
	Initializer<ServiceGraphModuleSettings> configureServiceGraph() {
		// add logical dependency so that service graph starts client only after it started the server
		return settings -> settings.addDependency(Key.of(RpcClient.class), Key.of(RpcServer.class));
	}

	@Provides
	Config config() {
		return Config.create()
				.with("rpc.server.port", "" + SERVICE_PORT)
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

	private int warmupRounds;
	private int benchmarkRounds;
	private int totalRequests;
	private int activeRequestsMin;
	private int activeRequestsMax;

	@Override
	protected void onStart() {
		warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds", WARMUP_ROUNDS);
		benchmarkRounds = config.get(ofInteger(), "benchmark.benchmarkRounds", BENCHMARK_ROUNDS);
		totalRequests = config.get(ofInteger(), "benchmark.totalRequests", TOTAL_REQUESTS);
		activeRequestsMin = config.get(ofInteger(), "benchmark.activeRequestsMin", ACTIVE_REQUESTS_MIN);
		activeRequestsMax = config.get(ofInteger(), "benchmark.activeRequestsMax", ACTIVE_REQUESTS_MAX);
	}

	@Override
	protected void run() throws Exception {
		benchmark("RPC");
	}

	/**
	 * First counter represents amount of sent requests, so we know when to stop sending them
	 * Second counter represents amount of completed requests(in another words completed will be incremented when
	 * request fails or completes successfully) so we know when to stop round of benchmark
	 */
	int sent;
	int completed;

	private void benchmark(String nameBenchmark) throws Exception {
		long time = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < warmupRounds; i++) {
			long roundTime = round();
			long rps = totalRequests * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; RPS : " + rps);
		}

		System.out.println("Start benchmarking " + nameBenchmark);

		for (int i = 0; i < benchmarkRounds; i++) {
			long roundTime = round();

			time += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = roundTime;
			}

			long rps = totalRequests * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; RPS : " + rps);
		}
		double avgTime = (double) time / benchmarkRounds;
		long requestsPerSecond = (long) (totalRequests / avgTime * 1000);
		System.out.println("Time: " + time + "ms; Average time: " + avgTime + "ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Requests per second: " + requestsPerSecond);
	}

	private long round() throws Exception {
		return eventloop.submit(this::roundCall).get();
	}

	private Promise<Long> roundCall() {
		SettablePromise<Long> promise = new SettablePromise<>();

		long start = System.currentTimeMillis();

		sent = 0;
		completed = 0;

		Callback<Integer> callback = new Callback<Integer>() {
			@Override
			public void accept(Integer result, @Nullable Throwable e) {
				if (e != null) return;
				completed++;

				int active = sent - completed;

				// Stop round
				if (completed == totalRequests) {
					promise.set(null);
					return;
				}

				if (active <= activeRequestsMin) {
					for (int i = 0; i < min(activeRequestsMax - active, totalRequests - sent); i++) {
						rpcClient.sendRequest(sent, this);
						sent++;
					}
				}
			}
		};

		for (int i = 0; i < min(activeRequestsMax, totalRequests); i++) {
			rpcClient.sendRequest(sent, callback);
			sent++;
		}

		return promise.map($ -> System.currentTimeMillis() - start);
	}

	public static void main(String[] args) throws Exception {
		RpcBenchmark benchmark = new RpcBenchmark();
		benchmark.launch(args);
	}
}
