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

import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.FsClient;
import io.datakernel.hashfs.HashFsClient;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class FileUploadExample {
	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(FileUploadExample.class);

		// Specifying file names: requested  - path to the file in your local file system,
		// result - file name after upload in remote fs
		String requestedFile = "pom.xml";
		String resultFile = "pom.xml";

		// Creating core components
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();

		// Specifying info about our bootstrap servers (see in previous example)
		ServerInfo server3 = new ServerInfo(3, new InetSocketAddress("127.0.0.1", 5573), 2.6);
		ServerInfo server4 = new ServerInfo(4, new InetSocketAddress("127.0.0.1", 5574), 8.0);
		final List<ServerInfo> bootstrap = Arrays.asList(server3, server4);

		// Creating client
		FsClient client = HashFsClient.createInstance(eventloop, bootstrap);

		// Creating producer that would stream file
		StreamProducer<ByteBuf> producer = StreamFileReader.readFileFully(eventloop, executor, 256, Paths.get(requestedFile));

		client.upload(resultFile, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Success");
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Failed");
			}
		});

		eventloop.run();
		executor.shutdown();
	}
}
