/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.remotefs.stress;

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.csp.process.ChannelSerializer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

class StressClient {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private InetSocketAddress address = new InetSocketAddress("localhost", 5560);
	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	private ExecutorService executor = Executors.newCachedThreadPool();

	private RemoteFsClient client = RemoteFsClient.create(eventloop, address);

	private static Random rand = new Random();

	private static final Path clientStorage = Paths.get("./test_data/clients_storage");

	private static Path downloads;

	private List<String> existingClientFiles = new ArrayList<>();
	private List<Operation> operations = new ArrayList<>();

	public void setup() throws IOException {
		downloads = clientStorage.resolve("downloads");
		Files.createDirectories(downloads);

		// create and upload
		operations.add(() -> {
			try {
				String fileName = createFile();
				existingClientFiles.add(fileName);

				Path file = clientStorage.resolve(fileName);

				ChannelFileReader.readFile(executor, file).withBufferSize(MemSize.kilobytes(16))
						.streamTo(client.uploader(fileName))
						.whenComplete(($, e) -> {
							if (e == null) {
								logger.info("Uploaded: " + fileName);
							} else {
								logger.info("Failed to upload: {}", e.getMessage());
							}
						});
			} catch (IOException e) {
				logger.info(e.getMessage());
			}
		});

		// download
		operations.add(() -> {
			if (existingClientFiles.isEmpty()) return;

			int index = rand.nextInt(existingClientFiles.size());
			String fileName = existingClientFiles.get(index);

			if (fileName == null) return;

			try {
				ChannelFileWriter consumer = ChannelFileWriter.create(executor, downloads.resolve(fileName));

				client.download(fileName, 0)
						.thenCompose(supplier -> supplier.streamTo(consumer))
						.whenComplete((supplier, e) -> {
							if (e == null) {
								logger.info("Downloaded: " + fileName);
							} else {
								logger.info("Failed to download: {}", e.getMessage());
							}
						});
			} catch (IOException e) {
				logger.info("Can't create consumer: {}", e.getMessage());
			}

		});

		// delete file
		operations.add(() -> {
			if (existingClientFiles.isEmpty()) return;

			int index = rand.nextInt(existingClientFiles.size());
			String fileName = existingClientFiles.get(index);

			client.deleteBulk(fileName).whenComplete(($, e) -> {
				if (e == null) {
					existingClientFiles.remove(fileName);
					logger.info("Deleted: " + fileName);
				} else {
					logger.info("Failed to delete: {}", e.getMessage());
				}
			});
		});

		// list file
		operations.add(() -> client.list().whenComplete((strings, e) -> {
			if (e == null) {
				logger.info("Listed: " + strings.size());
			} else {
				logger.info("Failed to list files: {}", e.getMessage());
			}
		}));

	}

	void start(int operationsQuantity, int maxDuration) throws IOException {
		setup();
		for (int i = 0; i < operationsQuantity; i++) {
			eventloop.delay(rand.nextInt(maxDuration), () ->
					operations.get(rand.nextInt(4)).go());
		}
		eventloop.run();
		executor.shutdown();
	}

	@FunctionalInterface
	private interface Operation {
		void go();
	}

	private String createFile() throws IOException {
		StringBuilder name = new StringBuilder();
		int nameLength = 5 + rand.nextInt(20);
		for (int i = 0; i < nameLength; i++) {
			name.append((char) (48 + rand.nextInt(74)));
		}

		Path file = clientStorage.resolve(name.toString());

		StringBuilder text = new StringBuilder();
		int textLength = rand.nextInt(1_000_000);
		for (int i = 0; i < textLength; i++) {
			text.append((char) (35 + rand.nextInt(60)));
			if (rand.nextBoolean())
				text.append("\r\n");
		}

		Files.write(file, text.toString().getBytes(StandardCharsets.UTF_8));

		return name.toString();
	}

	void uploadSerializedObject(int i) throws UnknownHostException {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		BinarySerializer<TestObject> binarySerializer = SerializerBuilder
				.create(classLoader)
				.build(TestObject.class);

		TestObject obj = new TestObject();
		obj.name = "someName";
		obj.ip = InetAddress.getLocalHost();

		StreamSupplier<TestObject> supplier = StreamSupplier.ofIterable(Collections.singletonList(obj));
		ChannelSerializer<TestObject> serializer = ChannelSerializer.create(binarySerializer)
				.withInitialBufferSize(ChannelSerializer.MAX_SIZE);

//		supplier.with(serializer).streamTo(
//				client.uploadStream("someName" + i));
//		eventloop.run();
		throw new UnsupportedOperationException("TODO");
	}

	void downloadSmallObjects(int i) {
		String name = "someName" + i;
		client.download(name, 0).whenComplete((supplier, e) -> {
			if (e != null) {
				logger.error("can't download", e);
			} else {
//				try {
//					supplier.streamTo(SerialFileWriter.create(executor, downloads.resolve(name)));
//				} catch (IOException e) {
//					logger.error("can't download", e);
//				}
				throw new UnsupportedOperationException("TODO");
			}
		});

		eventloop.run();
	}

	public static class TestObject {
		@Serialize(order = 0)
		public String name;

		@Serialize(order = 1)
		public InetAddress ip;
	}

}
