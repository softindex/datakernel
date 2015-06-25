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
import io.datakernel.stream.file.StreamFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This example demonstrates uploading file to the HashFS, which was set up in the previous example.
 * By default, HashFS creates 2 replicas of each file (and thus stores the same file on 3 servers, if they are available).
 * To run this example, find some file to upload and specify the path to it in program arguments.
 * Upon successful completion, you should be able to see your file uploaded to SERVER_STORAGE_PATH to 3 different folders.
 * On which server file will be saved is defined by file name.
 * For example, uploading 'random.tmp' to HashFS, which runs on 10 servers, will result in storing this file on servers #2, #4, #7.
 * To download the file you have just uploaded, proceed to the next example.
 */
public class HashFsFileUploadExample {
	private static final int FIRST_SERVER_PORT = 6732;
	private static final int SERVER_COUNT = 10;

	private static final Logger logger = LoggerFactory.getLogger(HashFsFileUploadExample.class);

	// Specify path to file to upload in the first argument
	public static void main(String[] args) {
		final String uploadFilePath = args[0];

		final List<ServerInfo> serverInfos = new ArrayList<>();

		for (int i = 0; i < SERVER_COUNT; i++) {
			ServerInfo info = new ServerInfo(new InetSocketAddress("127.0.0.1", FIRST_SERVER_PORT + i), i, 1);
			serverInfos.add(info);
		}

		final ExecutorService executor = Executors.newCachedThreadPool();

		final NioEventloop eventloop = new NioEventloop();

		// Create client
		final HashFsImpl client = HashFsImpl.createHashClient(eventloop, serverInfos);

		StreamConsumer<ByteBuf> consumer = client.upload(uploadFilePath);
		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, Paths.get(uploadFilePath));

		logger.info("Client started uploading file {}", uploadFilePath);
		producer.streamTo(consumer);

		eventloop.run();
		executor.shutdown();
	}
}
