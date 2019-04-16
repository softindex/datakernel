package io.datakernel.examples;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.ref.RefInt;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.stream.IntStream;

import static io.datakernel.async.Promise.of;
import static io.datakernel.util.CollectionUtils.set;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class FileChannelBenchmark {
	private static final int LIMIT_OPENS = 100_000;

	public static FileChannel operation() throws IOException {
		return FileChannel.open(Paths.get("test_data/file_test.txt"), set(READ, WRITE));
	}

	public static Promise<Long> benchmarkOpenBySequenceThroughExecutor() {
		RefInt refInt = new RefInt(0);
		SettablePromise<Long> result = new SettablePromise<>();
		long before = System.currentTimeMillis();

		benchmarkOpenBySequenceThroughExecutor(refInt, result, before);
		return result;
	}

	private static void benchmarkOpenBySequenceThroughExecutor(RefInt limit, SettablePromise<Long> result, long timeBefore) {
		Promise.ofBlockingCallable(FileChannelBenchmark::operation)
				.whenResult($ -> {
					if (limit.inc() != LIMIT_OPENS) {
						benchmarkOpenBySequenceThroughExecutor(limit, result, timeBefore);
					} else {
						result.set(System.currentTimeMillis() - timeBefore);
					}
				});
	}

	private static long syncBenchmark() {
		long before = System.currentTimeMillis();
		for (int i = 0; i < LIMIT_OPENS; i++) {
			try {
				operation();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return System.currentTimeMillis() - before;
	}

	public static Promise<Long> benchmarkOpenAllThroughExecutor() {
		long before = System.currentTimeMillis();
		return Promises.all(IntStream.range(0, LIMIT_OPENS)
				.mapToObj(i -> Promise.ofBlockingCallable(FileChannelBenchmark::operation)))
				.then($ -> of(System.currentTimeMillis() - before));
	}

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		AsyncSupplier<Void> benchmarkOpenAllThroughExecutor = () -> benchmarkOpenAllThroughExecutor()
				.whenResult(res -> {
					System.out.println("All at one time : " + res);
				}).toVoid();

		AsyncSupplier<Void> benchmarkOpenBySequenceThroughExecutor = () -> benchmarkOpenBySequenceThroughExecutor()
				.whenResult(res1 -> {
					System.out.println("All by sequence: " + res1);
				}).toVoid();


		AsyncSupplier<Void> syncBenchmark = () -> {
			long res3 = syncBenchmark();
			System.out.println("Sync : " + res3);
			return Promise.complete();
		};

		Promises.sequence(benchmarkOpenAllThroughExecutor,
				benchmarkOpenBySequenceThroughExecutor,
				syncBenchmark);

		eventloop.run();
	}

}
