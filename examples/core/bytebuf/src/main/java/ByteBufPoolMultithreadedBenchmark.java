import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.launcher.Launcher;

import static io.datakernel.config.converter.ConfigConverters.ofInteger;

public class ByteBufPoolMultithreadedBenchmark extends Launcher {

	final class ByteBufPoolAbuser implements Runnable {
		int number;
		int allocationSize;
		int iterations;

		public ByteBufPoolAbuser(int allocationSize, int iterations, int number) {
			this.allocationSize = allocationSize;
			this.iterations = iterations;
			this.number = number;
		}

		@Override
		public void run() {
			long start = System.currentTimeMillis();
			for (int i = 0; i < iterations; ++i) {
				ByteBuf buf = ByteBufPool.allocate(allocationSize);
				buf.recycle();
			}
			long res = System.currentTimeMillis() - start;
			System.out.println("Thread #" + number + " ends its job. Time : " + res + " ms.");
		}
	}

	private static final int ALLOCATION_SIZE = 32;
	private static final int THREADS = 32;
	private static final int ITERATIONS = 200_000;
	private final static int WARMUP_ROUNDS = 3;
	private final static int BENCHMARK_ROUNDS = 10;

	@Inject
	Config config;

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(Config.ofSystemProperties("config"));
	}

	private int allocationSize;
	private int threads;
	private int iterations;
	private int warmupRounds;
	private int measureRounds;

	@Override
	protected void onStart() throws Exception {
		this.allocationSize = config.get(ofInteger(), "benchmark.allocationSize", ALLOCATION_SIZE);
		this.threads = config.get(ofInteger(), "benchmark.threads", THREADS);
		this.iterations = config.get(ofInteger(), "benchmark.iterations", ITERATIONS);
		this.warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds", WARMUP_ROUNDS);
		this.measureRounds = config.get(ofInteger(), "benchmark.measureRounds", BENCHMARK_ROUNDS);
		super.onStart();
	}

	@Override
	protected void run() throws Exception {
		benchmark("ByteBuf Benchmark");
	}

	private void benchmark(String nameBenchmark) throws InterruptedException {
		long timeAllRounds = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < warmupRounds; i++) {
			double roundTime = round();
			long rps = iterations * 1000L * THREADS / Math.round(roundTime);
			System.out.println();
			System.out.println("Round: " + (i + 1) + "; ROUND TIME: " + Math.round(roundTime) + " ms; " +
					"RPS : " + rps);
			System.out.println();
		}

		System.out.println("Start benchmarking " + nameBenchmark);
		for (int i = 0; i < measureRounds; i++) {
			double roundTime = round();

			timeAllRounds += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = (long) roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = (long) roundTime;
			}
			long rps = iterations * 1000L * threads / Math.round(roundTime);
			System.out.println();
			System.out.println("Round: " + (i + 1) + "; ROUND TIME: " + Math.round(roundTime) + " ms; " +
					"RPS : " + rps);
			System.out.println();
		}

		double avgTime = (double) timeAllRounds / measureRounds;
		long avgRps = Math.round((iterations / avgTime * 1000L * threads));
		System.out.println("Time: " + timeAllRounds + "ms; Average time: " + avgTime + " ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Requests per second: " + avgRps);
	}

	private long round() throws InterruptedException {
		long start = System.currentTimeMillis();
		Thread[] abusers = new Thread[threads];
		for (int i = 0; i < threads; ++i) {
			abusers[i] = new Thread(new ByteBufPoolAbuser(allocationSize, iterations, i));
			abusers[i].start();
		}

		for (int i = 0; i < threads; ++i) {
			abusers[i].join();
		}

		return System.currentTimeMillis() - start;
	}

	public static void main(String[] args) throws Exception {
		Launcher benchmark = new ByteBufPoolMultithreadedBenchmark();
		benchmark.launch(args);
	}
}
