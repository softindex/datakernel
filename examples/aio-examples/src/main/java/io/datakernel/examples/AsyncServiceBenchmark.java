package io.datakernel.examples;

import com.google.inject.Inject;
import com.google.inject.Module;
import io.datakernel.aio.file.service.AioAsyncFileService;
import io.datakernel.async.*;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.UncheckedException;
import io.datakernel.file.AsyncFileService;
import io.datakernel.file.ExecutorAsyncFileService;
import io.datakernel.launcher.Launcher;
import io.datakernel.util.MemSize;
import io.datakernel.util.ref.RefInt;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.util.CollectionUtils.set;
import static io.datakernel.util.MemSize.*;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class AsyncServiceBenchmark extends Launcher {
	private static MemSize FILE_SIZE = gigabytes(1);
	private static MemSize SIZE_PART_READ = megabytes(1);
	private static MemSize SIZE_PART_WRITE = megabytes(1);
	private static int CONCURRENT_TASKS = 100;
	private static int ALL_TASKS = 1_000;
	private static boolean TEST_READ = true;
	private static boolean TEST_WRITE = true;
	private static int ROUNDS = 3;

	private static Path path = Paths.get("test_data/benchmarkFile.txt");

	@Inject
	Config config;

	static {
		System.setProperty("AsyncFileService.aio", "true");
	}

	private static void parsFlags(String[] args) {
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
				case "-c":
					CONCURRENT_TASKS = Integer.parseUnsignedInt(args[++i]);
					break;
				case "-a":
					ALL_TASKS = Integer.parseUnsignedInt(args[++i]);
					break;
				case "-s":
					FILE_SIZE = MemSize.of(Integer.parseUnsignedInt(args[++i]));
					break;
				case "-pr":
					SIZE_PART_READ = MemSize.of(Integer.parseUnsignedInt(args[++i]));
					break;
				case "-pwr":
					SIZE_PART_WRITE = MemSize.of(Integer.parseUnsignedInt(args[++i]));
					break;
				case "-r":
					ROUNDS = Integer.parseUnsignedInt(args[++i]);
					break;
				case "-tr":
					TEST_READ = Boolean.parseBoolean(args[++i]);
					break;
				case "-tw":
					TEST_WRITE = Boolean.parseBoolean(args[++i]);
					break;
				default:
					throw new IllegalArgumentException();
			}
		}
	}

	private int concurrentTasks;
	private int allTasks;
	private MemSize fileSize;
	private MemSize sizePartRead;
	private MemSize sizePartWrite;
	private boolean testRead;
	private boolean testWrite;
	private int rounds;

	@Override
	protected Collection<Module> getModules() {
		return Collections.singletonList(ConfigModule.create(Config.create()
				.with("benchmark.fileSize", "" + FILE_SIZE)
				.with("benchmark.concurrentTasks", "" + CONCURRENT_TASKS)
				.with("benchmark.allTasks", "" + ALL_TASKS)
				.with("benchmark.sizePartRead", "" + SIZE_PART_READ)
				.with("benchmark.sizePartWrite", "" + SIZE_PART_WRITE)
				.with("benchmark.testRead", "" + TEST_READ)
				.with("benchmark.testWrite", "" + TEST_WRITE)
				.with("benchmark.rounds", "" + ROUNDS)
		));
	}

	@Override
	protected void onStart() {
		concurrentTasks = config.get(ofInteger(), "benchmark.concurrentTasks");
		allTasks = config.get(ofInteger(), "benchmark.allTasks");
		fileSize = config.get(ofMemSize(), "benchmark.fileSize");
		sizePartRead = config.get(ofMemSize(), "benchmark.sizePartRead");
		sizePartWrite = config.get(ofMemSize(), "benchmark.sizePartWrite");
		testRead = config.get(ofBoolean(), "benchmark.testRead");
		testWrite = config.get(ofBoolean(), "benchmark.testWrite");
		testWrite = config.get(ofBoolean(), "benchmark.testWrite");
		rounds = config.get(ofInteger(), "benchmark.rounds");

		System.out.printf("All tasks = %d\n", allTasks);
		System.out.printf("Concurrent tasks = %d\n", concurrentTasks);
		System.out.printf("File size = %d\n", fileSize.toInt());
		System.out.printf("Size part read = %d\n", sizePartRead.toInt());
		System.out.printf("Size part write = %d\n", sizePartWrite.toInt());
		System.out.println("_____________________________________");
	}

	@SuppressWarnings("CastCanBeRemovedNarrowingVariableType")
	@Override
	protected void run() throws Exception {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		writeFullFile();

		AsyncFileService aioService = new AioAsyncFileService();
		((AioAsyncFileService) aioService).start();

		ExecutorAsyncFileService defaultService = new ExecutorAsyncFileService(newCachedThreadPool());

		Statistic statistic = new Statistic();
		AsyncSupplier<Void> aioWrite = () -> benchmarkWrite(aioService, path)
				.whenResult(res -> {
					System.out.println("AioAsyncFileService write time: " + res);
					statistic.aioWriteFullTime += res;
				})
				.toVoid();
		AsyncSupplier<Void> aioRead = () -> benchmarkRead(aioService, path)
				.whenResult(res -> {
					System.out.println("AioAsyncFileService read time: " + res);
					statistic.aioReadFullTime += res;
				})
				.toVoid();

		AsyncSupplier<Void> defaultWrite = () -> benchmarkWrite(defaultService, path)
				.whenResult(res -> {
					System.out.println("Default write time: " + res);
					statistic.defaultWriteFullTime += res;
				})
				.toVoid();
		AsyncSupplier<Void> defaultRead = () -> benchmarkRead(defaultService, path)
				.whenResult(res -> {
					System.out.println("Default read time: " + res);
					statistic.defaultReadFullTime += res;
				})
				.toVoid();

		RefInt countRound = new RefInt(0);

		round(countRound, defaultWrite, aioWrite, defaultRead, aioRead)
				.whenResult($ -> {
					System.out.println("\nAvg");
					System.out.println("Default write time " + (statistic.defaultWriteFullTime / rounds));
					System.out.println("Default read time " + (statistic.defaultReadFullTime / rounds));
					System.out.println("AIO write time " + (statistic.aioWriteFullTime / rounds));
					System.out.println("AIO read time " + (statistic.aioReadFullTime / rounds));

					try {
						((AioAsyncFileService) aioService).stop();
					} catch (Exception e) {
						e.printStackTrace();
					}
					shutdown();

				});

		eventloop.run();
	}

	private Promise<Void> round(RefInt roundCount,
								AsyncSupplier<Void> defaultWrite,
								AsyncSupplier<Void> aioWrite,
								AsyncSupplier<Void> defaultRead,
								AsyncSupplier<Void> aioRead) {

		return Promises.loop(0, AsyncPredicate.of(i -> i < rounds), i -> {
			System.out.println("\nRound #" + ++i);
			AsyncSupplier<Void> writeRound = () -> {
				if (testWrite) {
					return Promises.sequence(defaultWrite, aioWrite);
				}

				return Promise.of(null);
			};

			AsyncSupplier<Void> readRound = () -> {
				if (testRead) {
					return Promises.sequence(defaultRead, aioRead);
				}

				return Promise.of(null);
			};

			Integer finalI = i;
			return Promises.sequence(writeRound, readRound).then(($) -> Promise.of(finalI));
		}).toVoid();
	}


	private Promise<Long> benchmarkWrite(AsyncFileService service, Path path) {
		SettablePromise<Long> result = new SettablePromise<>();
		try {
			FileChannel channel = FileChannel.open(path, set(WRITE));

			RefInt completed = new RefInt(0);

			long before = System.currentTimeMillis();

			for (int i = 0; i < concurrentTasks; i++) {
				doOperation(completed, result, before, sizePartWrite, channel, service::write);
			}
		} catch (IOException e) {
			result.setException(e);
		}

		return result;
	}

	private Promise<Long> benchmarkRead(AsyncFileService service, Path path) {
		SettablePromise<Long> result = new SettablePromise<>();

		try {
			FileChannel channel = FileChannel.open(path, set(READ));
			RefInt completed = new RefInt(0);

			long before = System.currentTimeMillis();
			for (int i = 0; i < concurrentTasks; i++) {
				doOperation(completed, result, before, sizePartRead, channel, service::read);
			}
		} catch (IOException e) {
			result.setException(e);
		}

		return result;
	}

	private void doOperation(RefInt completed,
							 SettablePromise<Long> result,
							 long timeBefore,
							 MemSize sizePart,
							 FileChannel channel,
							 AsyncOperation operation) {

		int range = fileSize.toInt() - sizePart.toInt();
		if (range <= 0) {
			throw new UnsupportedOperationException("Inappropriate size of part");
		}
		byte[] array = new byte[sizePart.toInt()];

		long position = (long) (Math.random() * range);
		operation.doOperation(channel, position, array, 0, sizePart.toInt()).whenComplete(($, exc) -> {
			if (exc != null) {
				throw new UncheckedException(exc);
			}

			if (completed.inc() <= allTasks - concurrentTasks) {
				doOperation(
						completed, result, timeBefore,
						sizePart, channel, operation);
			} else {
				if (completed.get() == allTasks) {
					result.set(System.currentTimeMillis() - timeBefore);
				}
			}
		});
	}

	private void writeFullFile() throws IOException {
		FileOutputStream outputStream = new FileOutputStream(path.toFile());
		byte[] array = new byte[1024];

		for (int i = 0; i < array.length; i++) {
			array[i] = (byte) i;
		}

		for (int offset = 0; offset < fileSize.toInt(); offset += array.length) {
			outputStream.write(array, 0, Math.min(fileSize.toInt() - offset, array.length));
		}

		outputStream.flush();
		outputStream.close();
	}


	private interface AsyncOperation {
		Promise<Integer> doOperation(FileChannel channel, long position, byte[] array, int offset, int size);
	}

	public static void main(String[] args) throws Exception {
		parsFlags(args);

		Launcher benchmark = new AsyncServiceBenchmark();
		benchmark.launch(false, args);
	}

	private class Statistic {
		long defaultWriteFullTime;
		long aioWriteFullTime;
		long defaultReadFullTime;
		long aioReadFullTime;
	}
}
