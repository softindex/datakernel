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

package io.datakernel.simplefs;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.file.StreamFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Specify huge file ~10Gb, and 2 others of any size. They should be placed in directory mentioned as clientStorage
 * The test would start uploading huge file, then downloadedFile, then would receive stop command.
 * The later rejectedFile, send after server stop, as seen from its name would be rejected.
 * Finally the upload for the huge file should finish.
 */
public class StopAndHugeFileUploadTest {
	private static final Logger logger = LoggerFactory.getLogger(StopAndHugeFileUploadTest.class);

	private static final int LISTEN_PORT = 6432;
	private static final InetSocketAddress address = new InetSocketAddress("127.0.0.1", LISTEN_PORT);

	// Specify file names + paths
	private static String hugeFile;
	private static String downloadedFile;
	private static String rejectedFile;

	private static Path clientStorage;
	private static Path serverStorage;

	private static SimpleFsServer prepareServer(NioEventloop eventloop) throws IOException {

		final ExecutorService executor = Executors.newCachedThreadPool();
		final SimpleFsServer fileServer = SimpleFsServer.createServer(eventloop, serverStorage, executor);
		fileServer.setListenPort(LISTEN_PORT);
		try {
			fileServer.listen();
		} catch (IOException e) {
			logger.error("Can't start listen", e);
		}
		return fileServer;
	}

	public static void main(String[] args) throws IOException {
		final NioEventloop eventloop = new NioEventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();
		final SimpleFsClient client1 = new SimpleFsClient(eventloop, address);
		final SimpleFsClient client2 = new SimpleFsClient(eventloop, address);

		if (hugeFile == null || downloadedFile == null || rejectedFile == null || clientStorage == null || serverStorage == null) {
			logger.info("Specify first temporary paths and files");
		}

		final SimpleFsServer fileServer = prepareServer(eventloop);

		fileServer.start(new CompletionCallback() {
			@Override
			public void onComplete() {

				final StreamFileReader producer1 = StreamFileReader.readFileFully(eventloop, executor,
						16 * 1024, clientStorage.resolve(hugeFile));

				final StreamFileReader producer2 = StreamFileReader.readFileFully(eventloop, executor,
						16 * 1024, clientStorage.resolve(downloadedFile));

				final StreamFileReader producer3 = StreamFileReader.readFileFully(eventloop, executor,
						16 * 1024, clientStorage.resolve(rejectedFile));

				client1.upload(hugeFile, producer1, new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info(hugeFile + " downloaded successfully");
					}

					@Override
					public void onException(Exception e) {
						logger.error(hugeFile + " can't upload", e);
					}
				});

				eventloop.schedule(eventloop.currentTimeMillis() + 5000, new Runnable() {
					@Override
					public void run() {
						client2.upload(rejectedFile, producer3, new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.info("Should not happen");
							}

							@Override
							public void onException(Exception e) {
								logger.error("Can't upload " + rejectedFile, e);
							}
						});
					}
				});

				eventloop.schedule(eventloop.currentTimeMillis() + 2800, new Runnable() {
					@Override
					public void run() {
						client2.upload(downloadedFile, producer2, new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.info(downloadedFile + " downloaded successfully");
							}

							@Override
							public void onException(Exception e) {
								logger.error(downloadedFile + " can't upload", e);
							}
						});
					}
				});

				eventloop.schedule(eventloop.currentTimeMillis() + 3000, new Runnable() {
					@Override
					public void run() {
						fileServer.stop(new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.info("Stopped");
							}

							@Override
							public void onException(Exception e) {
								logger.error("Failed to stop", e);
							}
						});
					}
				});
			}

			@Override
			public void onException(Exception e) {
				logger.error("Failed to start", e);
			}
		});

		eventloop.run();
		executor.shutdownNow();
	}

}
