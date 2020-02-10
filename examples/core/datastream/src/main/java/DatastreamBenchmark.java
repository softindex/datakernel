import io.datakernel.config.Config;
import io.datakernel.datastream.*;
import io.datakernel.datastream.processor.StreamMapper;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.Transient;
import io.datakernel.di.core.InstanceProvider;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.promise.Promise;
import io.datakernel.service.ServiceGraphModule;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.datastream.StreamCapability.LATE_BINDING;
import static io.datakernel.di.module.Modules.combine;

@SuppressWarnings("WeakerAccess")
public class DatastreamBenchmark extends Launcher {
	private final static int TOTAL_ELEMENTS = 100_000_000;
	private final static int WARMUP_ROUNDS = 3;
	private final static int BENCHMARK_ROUNDS = 10;

	static final class IntegerStreamSupplier extends AbstractStreamSupplier<Integer> {
		private Integer integer;
		private int limit;

		public IntegerStreamSupplier(int limit) {
			this.integer = 0;
			this.limit = limit;
		}

		@Override
		protected void produce(AsyncProduceController async) {
			while (integer < limit) {
				StreamDataAcceptor<Integer> dataAcceptor = getCurrentDataAcceptor();
				dataAcceptor.accept(++integer);
			}
			sendEndOfStream();
		}

		@Override
		protected void onError(Throwable e) {
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	//region fields
	@Inject
	Eventloop eventloop;

	@Inject
	Config config;

	@Inject
	InstanceProvider<StreamSupplier<Integer>> inputProvider;

	@Inject
	InstanceProvider<StreamMapper<Integer, Integer>> mapperProvider;

	@Inject
	InstanceProvider<StreamConsumer<Integer>> outputProvider;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create().withCurrentThread();
	}

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}

	@Provides
	@Transient
	StreamSupplier<Integer> streamSupplier(Config config) {
		int limit = config.get(ofInteger(), "benchmark.totalElements", TOTAL_ELEMENTS);
		return new IntegerStreamSupplier(limit);
	}

	@Provides
	@Transient
	StreamMapper<Integer, Integer> mapper() {
		return StreamMapper.create(Function.identity());
	}

	@Provides
	@Transient
	StreamConsumer<Integer> streamConsumer() {
		return StreamConsumer.skip();
	}
	//endregion

	@Override
	protected Module getModule() {
		return combine(ServiceGraphModule.create());
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
		benchmark("Datastream");
	}

	private void benchmark(String nameBenchmark) throws Exception {
		long time = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < warmupRounds; i++) {
			long roundTime = round();
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
		StreamSupplier<Integer> input = inputProvider.get();
		StreamMapper<Integer, Integer> mapper = mapperProvider.get();
		StreamConsumer<Integer> output = outputProvider.get();
		long start = System.currentTimeMillis();
		return input
				.transformWith(mapper)
				.streamTo(output)
				.map($ -> System.currentTimeMillis() - start);
	}

	public static void main(String[] args) throws Exception {
		DatastreamBenchmark benchmark = new DatastreamBenchmark();
		benchmark.launch(args);
	}
}
