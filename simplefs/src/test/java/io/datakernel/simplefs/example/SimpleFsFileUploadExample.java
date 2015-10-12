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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.file.StreamFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This example demonstrates uploading file to SimpleFS, which was set up in the previous example.
 * To run this example, find some file to upload and specify the path to it in program arguments.
 * Upon successful completion, you should be able to see your file uploaded to SERVER_STORAGE_PATH (configured in the previous example).
 * To download the file you have just uploaded, proceed to the next example.
 */
public class SimpleFsFileUploadExample {
	private static final int SERVER_PORT = 6732;

	private static final Logger logger = LoggerFactory.getLogger(SimpleFsFileUploadExample.class);

	// Specify path to file to upload in the first argument
	public static void main(String[] args) {
		final String uploadFileName = args[0];
		final InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", SERVER_PORT);
		final ExecutorService executor = Executors.newCachedThreadPool();

		final NioEventloop eventloop = new NioEventloop();

		// Create client
		SimpleFsClient client = new SimpleFsClient(eventloop, serverAddress);

		final StreamFileReader producer =
				StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, Paths.get(uploadFileName));

		StreamConsumer<ByteBuf> consumer = client.upload(uploadFileName);
		consumer.addCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Client uploaded file {}", uploadFileName);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Can't upload file {}", uploadFileName, exception);
			}
		});

		eventloop.run();
		executor.shutdown();
	}
}
