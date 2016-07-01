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

package io.datakernel.simplefs.stress;

import com.google.common.base.Charsets;
import io.datakernel.StreamTransformerWithCounter;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.simplefs.SimpleFsClient;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

class StressClient {
	private static final Logger logger = LoggerFactory.getLogger(StressClient.class);
	private InetSocketAddress address = new InetSocketAddress(5560);
	private Eventloop eventloop = new Eventloop();
	private ExecutorService executor = Executors.newCachedThreadPool();

	private SimpleFsClient client = new SimpleFsClient(eventloop, address);

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

					client.upload(fileName, producer, new CompletionCallback() {
						@Override
						public void onComplete() {
							logger.info("Uploaded: " + fileName);
						}

						@Override
						public void onException(Exception e) {
							logger.info("Failed to upload: {}", e.getMessage());
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
					consumer.setFlushCallback(new CompletionCallback() {
						@Override
						public void onComplete() {
							logger.info("Downloaded: " + fileName);
						}

						@Override
						public void onException(Exception e) {
							logger.info("Failed to download: {}", e.getMessage());
						}
					});
					client.download(fileName, 0, new ResultCallback<StreamTransformerWithCounter>() {
						@Override
						public void onResult(StreamTransformerWithCounter result) {
							result.getOutput().streamTo(consumer);
						}

						@Override
						public void onException(Exception e) {
							logger.info("can't download: {}", e.getMessage());
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

				client.delete(fileName, new CompletionCallback() {
					@Override
					public void onComplete() {
						existingClientFiles.remove(fileName);
						logger.info("Deleted: " + fileName);
					}

					@Override
					public void onException(Exception e) {
						logger.info("Failed to delete: {}", e.getMessage());
					}
				});
			}
		});

		// list file
		operations.add(new Operation() {
			@Override
			public void go() {
				client.list(new ResultCallback<List<String>>() {
					@Override
					public void onResult(List<String> result) {
						logger.info("Listed: " + result.size());
					}

					@Override
					public void onException(Exception e) {
						logger.info("Failed to list files: {}", e.getMessage());
					}
				});
			}
		});

	}

	void start(int operationsQuantity, int maxDuration) throws IOException {
		setup();
		for (int i = 0; i < operationsQuantity; i++) {
			eventloop.schedule(eventloop.currentTimeMillis() + rand.nextInt(maxDuration), new Runnable() {
				@Override
				public void run() {
					operations.get(rand.nextInt(4)).go();
				}
			});
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

		Files.write(file, text.toString().getBytes(Charsets.UTF_8));

		return name.toString();
	}

	void uploadSerializedObject(int i) throws UnknownHostException {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		BufferSerializer<TestObject> bufferSerializer = SerializerBuilder
				.newDefaultInstance(classLoader)
				.create(TestObject.class);

		TestObject obj = new TestObject();
		obj.name = "someName";
		obj.ip = InetAddress.getLocalHost();

		StreamProducer<TestObject> producer = StreamProducers.ofIterable(eventloop, Collections.singletonList(obj));
		StreamBinarySerializer<TestObject> serializer =
				new StreamBinarySerializer<>(eventloop, bufferSerializer, StreamBinarySerializer.MAX_SIZE, StreamBinarySerializer.MAX_SIZE, 1000, false);

		producer.streamTo(serializer.getInput());
		client.upload("someName" + i, serializer.getOutput(), ignoreCompletionCallback());
		eventloop.run();
	}

	void downloadSmallObjects(int i) {
		final String name = "someName" + i;
		client.download(name, 0, new ResultCallback<StreamTransformerWithCounter>() {
			@Override
			public void onResult(StreamTransformerWithCounter result) {
				try {
					StreamFileWriter writer = StreamFileWriter.create(eventloop, executor, downloads.resolve(name));
					result.getOutput().streamTo(writer);
				} catch (IOException e) {
					this.onException(e);
				}
			}

			@Override
			public void onException(Exception e) {
				logger.error("can't download", e);
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
