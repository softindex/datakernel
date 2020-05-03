import io.datakernel.config.Config;
import io.datakernel.csp.AbstractChannelSupplier;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.di.InstanceProvider;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.Transient;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.promise.Promise;
import io.datakernel.service.ServiceGraphModule;

import java.util.function.Function;

import static io.datakernel.config.ConfigConverters.ofInteger;

@SuppressWarnings("WeakerAccess")
public class CspBenchmark extends Launcher {
	private final static int TOTAL_ELEMENTS = 50_000_000;
	private final static int WARMUP_ROUNDS = 3;
	private final static int BENCHMARK_ROUNDS = 10;

	static final class IntegerChannelSupplier extends AbstractChannelSupplier<Integer> {
		private Integer integer;
		private final int limit;

		public IntegerChannelSupplier(int limit) {
			this.integer = 0;
			this.limit = limit;
		}

		@Override
		protected Promise<Integer> doGet() {
			return Promise.of(integer < limit ? ++integer : null);
		}
	}

	//region fields
	@Inject
	Eventloop eventloop;

	@Inject
	Config config;

	@Inject
	InstanceProvider<ChannelSupplier<Integer>> inputProvider;

	@Inject
	InstanceProvider<ChannelConsumer<Integer>> outputProvider;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Provides
	@Transient
	ChannelSupplier<Integer> channelSupplier(Config config) {
		return new IntegerChannelSupplier(config.get(ofInteger(), "benchmark.totalElements", TOTAL_ELEMENTS));
	}

	@Provides
	@Transient
	ChannelConsumer<Integer> channelConsumer() {
		return ChannelConsumer.ofConsumer(x -> {});
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	private int warmupRounds;
	private int benchmarkRounds;
	private int totalElements;

	@Override
	protected void onStart() {
		warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds", WARMUP_ROUNDS);
		benchmarkRounds = config.get(ofInteger(), "benchmark.benchmarkRounds", BENCHMARK_ROUNDS);
		totalElements = config.get(ofInteger(), "benchmark.totalElements", TOTAL_ELEMENTS);
	}

	@Override
	protected void run() throws Exception {
		benchmark("CSP Channel");
	}

	private void benchmark(String nameBenchmark) throws Exception {
		long time = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < warmupRounds; i++) {
			long roundTime = round();
			if (roundTime == 0) roundTime++;
			long rps = totalElements * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; OPS : " + rps);
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
			if (roundTime == 0) roundTime++;
			long rps = totalElements * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; OPS : " + rps);
		}
		double avgTime = (double) time / benchmarkRounds;
		long requestsPerSecond = (long) (totalElements / avgTime * 1000);
		System.out.println("Time: " + time + "ms; Average time: " + avgTime + "ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Operations per second: " + requestsPerSecond);
	}

	private long round() throws Exception {
		return eventloop.submit(this::roundCall).get();
	}

	private Promise<Long> roundCall() {
		ChannelSupplier<Integer> input = inputProvider.get();
		ChannelConsumer<Integer> output = outputProvider.get();
		long start = System.currentTimeMillis();
		return input.map(Function.identity())
				.streamTo(output)
				.map($ -> System.currentTimeMillis() - start);
	}

	public static void main(String[] args) throws Exception {
		CspBenchmark benchmark = new CspBenchmark();
		benchmark.launch(args);
	}
}
