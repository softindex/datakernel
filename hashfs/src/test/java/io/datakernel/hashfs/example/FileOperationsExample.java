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
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.Config;
import io.datakernel.hashfs.FsClient;
import io.datakernel.hashfs.ServerFactory;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.datakernel.stream.file.StreamFileReader.readFileFully;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class FileOperationsExample {
	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(FileOperationsExample.class);

		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();

		ServerInfo server3 = new ServerInfo(3, new InetSocketAddress("127.0.0.1", 5573), 1);
		ServerInfo server4 = new ServerInfo(4, new InetSocketAddress("127.0.0.1", 5574), 1);

		final Set<ServerInfo> bootstrap = new HashSet<>(Arrays.asList(server3, server4));

		FsClient client = ServerFactory.getClient(eventloop, bootstrap, Config.getDefaultConfig());
		StreamProducer<ByteBuf> producer = readFileFully(eventloop, newCachedThreadPool(), 10 * 1024,
				Paths.get("./test/client_storage/ptt.txt"));

		//////////////////////////////////////////////////////////////////////////////////////////////////////////UPLOAD
		client.upload("java.txt", producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Successfully uploaded: c++.txt");
			}

			@Override
			public void onException(Exception e) {
				logger.info("Failed to upload: c++.txt");
			}
		});

		////////////////////////////////////////////////////////////////////////////////////////////////////////////LIST
		client.list(new ResultCallback<List<String>>() {
			@Override
			public void onResult(List<String> result) {
				logger.info("Received list of files on server: {}", result);
			}

			@Override
			public void onException(Exception e) {
				logger.info("Can't get list of files on server");
			}
		});

		////////////////////////////////////////////////////////////////////////////////////////////////////////DOWNLOAD
		StreamFileWriter consumer = StreamFileWriter.createFile(eventloop, executor,
				Paths.get("./test/client_storage/lang.txt"));
		consumer.setFlushCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Downloaded");
			}

			@Override
			public void onException(Exception e) {
				logger.error("Failed to download", e);
			}
		});
		client.download("java.txt", consumer);

		//////////////////////////////////////////////////////////////////////////////////////////////////////////DELETE
		client.delete("java.txt", new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Deleted: ptt.txt");
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't delete: ptt.txt", e);
			}
		});

		eventloop.run();
		executor.shutdown();
	}
}
