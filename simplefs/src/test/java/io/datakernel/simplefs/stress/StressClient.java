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
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.simplefs.StopAndHugeFileUploadTest;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class StressClient {
	private static final Logger logger = LoggerFactory.getLogger(StopAndHugeFileUploadTest.class);

	private static final int SERVER_PORT = 6732;
	private static final InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", SERVER_PORT);

	private NioEventloop eventloop = new NioEventloop();
	private ExecutorService executor = Executors.newCachedThreadPool();
	private SimpleFsClient client = new SimpleFsClient(eventloop, serverAddress);

	private static Random rand = new Random();

	private static final Path clientStorage = Paths.get("./tmp/stress_test/clients_storage");
	private static Path downloads;

	List<String> existingClientFiles = new ArrayList<>();
	List<Operation> operations = new ArrayList<>();

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

				StreamFileWriter consumer =
						StreamFileWriter.createFile(eventloop, executor, downloads.resolve(fileName));
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

				client.download(fileName, consumer);
			}
		});

		// delete file
		operations.add(new Operation() {
			@Override
			public void go() {
				if (existingClientFiles.isEmpty()) return;

				int index = rand.nextInt(existingClientFiles.size());
				final String fileName = existingClientFiles.get(index);

				client.deleteFile(fileName, new CompletionCallback() {
					@Override
					public void onComplete() {
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
				client.listFiles(new ResultCallback<List<String>>() {
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

	public void start(int operationsQuantity, int maxDuration) throws IOException {
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

	interface Operation {
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
}
