package io.datakernel.remotefs.stress;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.stream.StreamConsumer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.file.AsyncFile.open;
import static io.datakernel.stream.file.StreamFileWriter.CREATE_OPTIONS;
import static io.datakernel.stream.file.StreamFileWriter.create;

public class StressDownload {
	private static final int OPERATIONS_QUANTITY = 10 * 1024;
	private static final int FILE_MAX_SIZE = 1024;

	private static final Path CLIENT_STORAGE = Paths.get("./test_data/client_storage");

	private static Random rand = new Random();
	public static final List<String> FILES = new ArrayList<>();

	public static void main(String[] args) throws IOException, InterruptedException {

		Files.createDirectories(CLIENT_STORAGE);

		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		final ExecutorService executor = Executors.newCachedThreadPool();

		final int[] failures = new int[1];

		RemoteFsClient client = RemoteFsClient.create(eventloop, new InetSocketAddress("localhost", 5560));

		for (int i = 0; i < OPERATIONS_QUANTITY; i++) {
			FILES.add(createFile());
		}

		for (int i = 0; i < OPERATIONS_QUANTITY; i++) {
			final String file = FILES.get(rand.nextInt(OPERATIONS_QUANTITY));
			client.download(file, 0).whenComplete((producer, throwable) -> {
				if (throwable == null) {
					try {
						final AsyncFile open = open(eventloop, executor, CLIENT_STORAGE.resolve(file), CREATE_OPTIONS);
						StreamConsumer<ByteBuf> consumer = create(eventloop, open);
						producer.streamTo(consumer);
					} catch (IOException e) {
						failures[0]++;
					}
				} else {
					failures[0]++;
				}
			});

			eventloop.run();
		}

		executor.shutdown();
		System.out.println("Failures: " + failures[0]);
	}

	public static String createFile() throws IOException {
		String name = Integer.toString(rand.nextInt(Integer.MAX_VALUE));
		Path file = StressServer.STORAGE_PATH.resolve(name);
		byte[] bytes = new byte[FILE_MAX_SIZE];
		rand.nextBytes(bytes);
		Files.write(file, bytes);
		return name;
	}
}
