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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This example demonstrates downloading file from SimpleFS.
 * For that we assume that SimpleFS has been set up and file has been uploaded.
 * Specify the name of file to download as a program argument (same as the one you uploaded in the previous example).
 * If run successfully, the requested file will be downloaded to ./test/ (you may change this setting).
 */
public class SimpleFsFileDownloadExample {
	private static final int SERVER_PORT = 6732;
	private static final Path DOWNLOAD_PATH = Paths.get("./test/");

	private static final Logger logger = LoggerFactory.getLogger(SimpleFsFileDownloadExample.class);

	// Specify the name of file to download in the first argument
	public static void main(String[] args) {
		final String downloadFileName = args[0];
		final InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", SERVER_PORT);
		final ExecutorService executor = Executors.newCachedThreadPool();

		final NioEventloop eventloop = new NioEventloop();

		SimpleFsClient client = new SimpleFsClient(eventloop);

		client.read(serverAddress, downloadFileName, new ResultCallback<StreamProducer<ByteBuf>>() {
			@Override
			public void onResult(StreamProducer<ByteBuf> result) {
				logger.info("Client started downloading file {}", downloadFileName);
				StreamFileWriter consumer =
						StreamFileWriter.createFile(eventloop, executor, DOWNLOAD_PATH.resolve(downloadFileName));
				result.streamTo(consumer);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Can't download file {}", downloadFileName, exception);
			}
		});

		eventloop.run();
		executor.shutdown();
	}
}
