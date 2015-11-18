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

package io.datakernel.simplefs.example;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.stream.file.StreamFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This example demonstrates uploading file to SimpleFS, which was set up in the previous example.
 * To run this example, find some file to upload and specify the path to it in 'fileName' variable.
 * Upon successful completion, you should be able to see your file uploaded to SERVER_STORAGE_PATH (configured in the previous example).
 * To download the file you have just uploaded, proceed to the next example.
 */
public class SimpleFsFileUploadExample {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsFileUploadExample.class);

	private static final int SERVER_PORT = 6732;
	private static final Path CLIENT_STORAGE = Paths.get("./");

	public static void main(String[] args) {
		final InetSocketAddress address = new InetSocketAddress(SERVER_PORT);
		final ExecutorService executor = Executors.newCachedThreadPool();
		final NioEventloop eventloop = new NioEventloop();

		final String fileName = "test.txt";

		// Create client
		SimpleFsClient client = SimpleFsClient.buildInstance(eventloop, address).build();

		final StreamFileReader producer =
				StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, CLIENT_STORAGE.resolve(fileName));

		client.upload(fileName, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Client uploaded file {}", fileName);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't upload file {}", fileName, e);
			}
		});

		eventloop.run();
		executor.shutdown();
	}
}
