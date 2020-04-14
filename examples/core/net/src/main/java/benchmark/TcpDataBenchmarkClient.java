package benchmark;

import io.datakernel.config.Config;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.datastream.AbstractStreamSupplier;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.csp.ChannelDeserializer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.net.AsyncTcpSocketNio;
import io.datakernel.promise.Promise;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.launchers.initializers.Initializers.ofAsyncComponents;
import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;

@SuppressWarnings("WeakerAccess")
public class TcpDataBenchmarkClient extends Launcher {
	private final static int TOTAL_ELEMENTS = 100_000_000;
	private final static int WARMUP_ROUNDS = 3;
	private final static int BENCHMARK_ROUNDS = 10;

	private int items;
	private int warmupRounds;
	private int benchmarkRounds;

	@Inject
	@Named("benchmark")
	Eventloop benchmarkEventloop;

	@Inject
	@Named("client")
	Eventloop clientEventloop;

	@Inject
	Config config;

	@Provides
	@Named("benchmark")
	Eventloop benchmarkEventloop() { return Eventloop.create(); }

	@Provides
	@Named("client")
	Eventloop clientEventloop() { return Eventloop.create(); }

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create()
				.initialize(ofAsyncComponents());
	}

	@Override
	protected void onStart() {
		this.items = config.get(ofInteger(), "benchmark.totalElements", TOTAL_ELEMENTS);
		this.warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds", WARMUP_ROUNDS);
		this.benchmarkRounds = config.get(ofInteger(), "benchmark.benchmarkRounds", BENCHMARK_ROUNDS);
	}

	@Override
	protected void run() throws Exception {
		benchmark(this::round, "TCP Echo Server");
	}

	private void benchmark(Supplier<Promise<Long>> function, String nameBenchmark) throws Exception {
		long timeAllRounds = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < warmupRounds; i++) {
			long roundTime = round(function);
			long rps = roundTime != 0 ? (items * 1000L / roundTime) : 0;
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

			long rps = items * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; RPS : " + rps);
		}

		long avgRoundTime = timeAllRounds / benchmarkRounds;
		long avgRps = avgRoundTime != 0 ? (items * benchmarkRounds * 1000L / timeAllRounds) : 0;
		System.out.println("Total time: " + timeAllRounds + "ms; Average round time: " + avgRoundTime + "ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Average RPS: " + avgRps);
	}

	private long round(Supplier<Promise<Long>> function) throws Exception {
		return benchmarkEventloop.submit(function).get();
	}

	private Promise<Long> round() {
		long start = System.currentTimeMillis();

		InetSocketAddress address = config.get(ofInetSocketAddress(), "echo.address", new InetSocketAddress(9001));
		int limit = config.get(ofInteger(), "benchmark.totalElements", TOTAL_ELEMENTS);

		return AsyncTcpSocketNio.connect(address)
				.then(socket -> {
					StreamSupplierOfSequence.create(limit)
							.transformWith(ChannelSerializer.create(INT_SERIALIZER))
							.streamTo(ChannelConsumer.ofSocket(socket));

					return ChannelSupplier.ofSocket(socket)
							.transformWith(ChannelDeserializer.create(INT_SERIALIZER))
							.streamTo(StreamConsumer.skip())
							.whenComplete(socket::close)
							.map($ -> System.currentTimeMillis() - start);
				});
	}

	static final class StreamSupplierOfSequence extends AbstractStreamSupplier<Integer> {
		private int value;
		private final int limit;

		private StreamSupplierOfSequence(int limit) {
			this.value = 0;
			this.limit = limit;
		}

		public static StreamSupplierOfSequence create(int limit) {
			return new StreamSupplierOfSequence(limit);
		}

		@Override
		protected void onResumed() {
			while (value < limit) {
				StreamDataAcceptor<Integer> dataAcceptor = getDataAcceptor();
				if (dataAcceptor == null) {
					return;
				}
				dataAcceptor.accept(++value);
			}
			sendEndOfStream();
		}
	}

	public static void main(String[] args) throws Exception {
		Launcher benchmark = new TcpDataBenchmarkClient();
		benchmark.launch(args);
	}

}
