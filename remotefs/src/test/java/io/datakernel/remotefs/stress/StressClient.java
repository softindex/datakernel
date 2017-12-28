/*
 * Copyright (C) 2015 SoftIndex LLC.
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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinarySerializer;
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
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private InetSocketAddress address = new InetSocketAddress("localhost", 5560);
	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
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
		operations.add(new Operation() {
			@Override
			public void go() {
				try {
					final String fileName = createFile();
					existingClientFiles.add(fileName);

					Path file = clientStorage.resolve(fileName);
					StreamFileReader producer =
							StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, file);

					StreamConsumerWithResult<ByteBuf, Void> consumer = client.uploadStream(fileName);
					producer.streamTo(consumer);
					consumer.getResult().whenComplete(($, throwable) -> {
						if (throwable == null) {
							logger.info("Uploaded: " + fileName);
						} else {
							logger.info("Failed to upload: {}", throwable.getMessage());
						}
					});
				} catch (IOException e) {
					logger.info(e.getMessage());
				}
			}
		});

		// download
		operations.add(new Operation() {
			@Override
			public void go() {
				if (existingClientFiles.isEmpty()) return;

				int index = rand.nextInt(existingClientFiles.size());
				final String fileName = existingClientFiles.get(index);

				if (fileName == null) return;

				try {
					final StreamFileWriter consumer = StreamFileWriter.create(eventloop, executor, downloads.resolve(fileName));
					consumer.getFlushStage().whenComplete(($, throwable) -> {
						if (throwable == null) logger.info("Downloaded: " + fileName);
						else logger.info("Failed to download: {}", throwable.getMessage());
					});

					client.download(fileName, 0).whenComplete((producer, throwable) -> {
						if (throwable == null) {
							producer.streamTo(consumer);
						} else {
							logger.info("can't download: {}", throwable.getMessage());
						}
					});
				} catch (IOException e) {
					logger.info("can't create consumer: {}", e.getMessage());
				}

			}
		});

		// delete file
		operations.add(new Operation() {
			@Override
			public void go() {
				if (existingClientFiles.isEmpty()) return;

				int index = rand.nextInt(existingClientFiles.size());
				final String fileName = existingClientFiles.get(index);

				client.delete(fileName).whenComplete(($, throwable) -> {
					if (throwable == null) {
						existingClientFiles.remove(fileName);
						logger.info("Deleted: " + fileName);
					} else {
						logger.info("Failed to delete: {}", throwable.getMessage());
					}
				});
			}
		});

		// list file
		operations.add(() -> client.list().whenComplete((strings, throwable) -> {
			if (throwable == null) {
				logger.info("Listed: " + strings.size());
			} else {
				logger.info("Failed to list files: {}", throwable.getMessage());
			}
		}));

	}

	void start(int operationsQuantity, int maxDuration) throws IOException {
		setup();
		for (int i = 0; i < operationsQuantity; i++) {
			eventloop.schedule(eventloop.currentTimeMillis() + rand.nextInt(maxDuration), () ->
					operations.get(rand.nextInt(4)).go());
		}
		eventloop.run();
		executor.shutdown();
	}

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
		BufferSerializer<TestObject> bufferSerializer = SerializerBuilder
				.create(classLoader)
				.build(TestObject.class);

		TestObject obj = new TestObject();
		obj.name = "someName";
		obj.ip = InetAddress.getLocalHost();

		StreamProducer<TestObject> producer = StreamProducers.ofIterable(eventloop, Collections.singletonList(obj));
		StreamBinarySerializer<TestObject> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer)
				.withDefaultBufferSize(StreamBinarySerializer.MAX_SIZE);

		producer.streamTo(serializer.getInput());
		serializer.getOutput().streamTo(client.uploadStream("someName" + i));
		eventloop.run();
	}

	void downloadSmallObjects(int i) {
		final String name = "someName" + i;
		client.download(name, 0).whenComplete((producer, throwable) -> {
			if (throwable != null) {
				logger.error("can't download", throwable);
			} else {
				try {
					StreamFileWriter writer = StreamFileWriter.create(eventloop, executor, downloads.resolve(name));
					producer.streamTo(writer);
				} catch (IOException e) {
					logger.error("can't download", e);
				}
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
