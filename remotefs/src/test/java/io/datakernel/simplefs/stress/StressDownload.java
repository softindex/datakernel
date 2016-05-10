package io.datakernel.simplefs.stress;

import io.datakernel.StreamTransformerWithCounter;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.simplefs.SimpleFsClient;
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

		final Eventloop eventloop = new Eventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();

		final int[] failures = new int[1];

		SimpleFsClient client = new SimpleFsClient(eventloop, new InetSocketAddress(5560));

		for (int i = 0; i < OPERATIONS_QUANTITY; i++) {
			FILES.add(createFile());
		}

		for (int i = 0; i < OPERATIONS_QUANTITY; i++) {
			final String file = FILES.get(rand.nextInt(OPERATIONS_QUANTITY));
			client.download(file, 0, new ResultCallback<StreamTransformerWithCounter>() {
				@Override
				public void onResult(StreamTransformerWithCounter result) {
					try {
						StreamConsumer<ByteBuf> consumer = create(eventloop,
								open(eventloop, executor,
										CLIENT_STORAGE.resolve(file),
										CREATE_OPTIONS));
						result.getOutput().streamTo(consumer);
					} catch (IOException e) {
						onException(e);
					}
				}

				@Override
				public void onException(Exception e) {
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
