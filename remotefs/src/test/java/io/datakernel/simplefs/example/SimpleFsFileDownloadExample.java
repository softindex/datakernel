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

import io.datakernel.StreamTransformerWithCounter;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This example demonstrates downloading file from SimpleFS.
 * For that we assume that SimpleFS has been set up and file has been uploaded.
 * Specify the name of file to download as a program argument (same as the one you uploaded in the previous example).
 * If run successfully, the requested file will be downloaded to ./test_data/ (you may change this setting).
 */
public class SimpleFsFileDownloadExample {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsFileDownloadExample.class);

	private static final int SERVER_PORT = 6732;
	private static final Path CLIENT_STORAGE = Paths.get("./test_data");

	public static void main(String[] args) throws IOException {
		final ExecutorService executor = Executors.newCachedThreadPool();
		final Eventloop eventloop = new Eventloop();

		String requiredFile = "example.txt";
		final String downloadedFile = "downloaded_example.txt";

		// creating client
		SimpleFsClient client = SimpleFsClient.newInstance(eventloop, new InetSocketAddress(SERVER_PORT));

		// downloading file
		client.download(requiredFile, 0, new ResultCallback<StreamTransformerWithCounter>() {
			@Override
			public void onResult(StreamTransformerWithCounter result) {
				try {

					// opening consumer - stream that would save the file on disk
					StreamFileWriter consumer = StreamFileWriter.create(eventloop, executor, CLIENT_STORAGE.resolve(downloadedFile));

					// setting flush callback that would notify us on file being fully downloaded
					consumer.setFlushCallback(new ForwardingCompletionCallback(this) {
						@Override
						public void onComplete() {
							logger.info("Client finished writing file {} on disk", downloadedFile);
						}
					});
					// streaming file from net to disk
					result.getOutput().streamTo(consumer);
				} catch (IOException e) {
					onException(e);
				}

			}

			@Override
			public void onException(Exception e) {
				logger.info("Can't download file: {}", e.getMessage());
			}
		});

		eventloop.run();
		executor.shutdown();
	}
}
