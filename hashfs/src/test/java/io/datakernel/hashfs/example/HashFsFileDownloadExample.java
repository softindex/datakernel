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

package io.datakernel.hashfs.example;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.HashFsImpl;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This example demonstrates downloading file from HashFS. This example assumes HashFS has been set up and file has been uploaded.
 * Specify the name of file to download as a command argument (same as the one you uploaded in previous example).
 * If run successfully, the requested file will be downloaded to ./test/ (you may change this setting).
 */
public class HashFsFileDownloadExample {
	private static final int FIRST_SERVER_PORT = 6732;
	private static final int SERVER_COUNT = 10;
	private static final String DOWNLOAD_DIRECTORY = "./test/";

	private static final Logger logger = LoggerFactory.getLogger(HashFsFileDownloadExample.class);

	// Specify the name of file to download in the first argument
	public static void main(String[] args) {
		final String downloadFileName = args[0];

		final List<ServerInfo> serverInfos = new ArrayList<>();

		for (int i = 0; i < SERVER_COUNT; i++) {
			ServerInfo info = new ServerInfo(new InetSocketAddress("127.0.0.1", FIRST_SERVER_PORT + i), i, 1);
			serverInfos.add(info);
		}

		final ExecutorService executor = Executors.newCachedThreadPool();

		final NioEventloop eventloop = new NioEventloop();

		// Create client
		final HashFsImpl client = HashFsImpl.createHashClient(eventloop, serverInfos);

		StreamProducer<ByteBuf> producer = client.download(downloadFileName);
		Path destination = Paths.get(DOWNLOAD_DIRECTORY);
		StreamConsumer<ByteBuf> diskWrite = StreamFileWriter.createFile(eventloop, executor,
				destination.resolve(downloadFileName), true);
		producer.streamTo(diskWrite);

		eventloop.run();
		executor.shutdown();
	}
}
