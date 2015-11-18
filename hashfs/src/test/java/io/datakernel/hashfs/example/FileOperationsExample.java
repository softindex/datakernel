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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.HashFsClient;
import io.datakernel.hashfs.FsClient;
import io.datakernel.hashfs.RfsConfig;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.stream.file.StreamFileReader.readFileFully;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class FileOperationsExample {
	private static NioEventloop eventloop = new NioEventloop();

	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(FileOperationsExample.class);

		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();

		ServerInfo server3 = new ServerInfo(3, new InetSocketAddress("127.0.0.1", 5573), 1);
		ServerInfo server4 = new ServerInfo(4, new InetSocketAddress("127.0.0.1", 5574), 1);

		final List<ServerInfo> bootstrap = Arrays.asList(server3, server4);

		FsClient client = HashFsClient.createInstance(eventloop, bootstrap, RfsConfig.getDefaultConfig());

		upload("./test/client_storage/ptt.txt", "java.txt", client, ignoreCompletionCallback());
		list(client, AsyncCallbacks.<List<String>>ignoreResultCallback());
		download("./test/client_storage/lang.txt", "java.txt", client, ignoreCompletionCallback());
		delete("java.txt", client, ignoreCompletionCallback());

		eventloop.run();
		executor.shutdown();
	}

	private static void delete(String src, FsClient client, CompletionCallback callback) {
		client.delete(src, callback);
	}

	private static void download(String src, String dst, FsClient client, CompletionCallback callback) {
		StreamFileWriter consumer = StreamFileWriter.createFile(eventloop, newCachedThreadPool(),
				Paths.get(src));
		consumer.setFlushCallback(callback);
		client.download(dst, consumer);
	}

	private static void list(FsClient client, ResultCallback<List<String>> callback) {
		client.list(callback);
	}

	private static void upload(String src, String dst, FsClient client, final CompletionCallback callback) {
		StreamProducer<ByteBuf> producer = readFileFully(eventloop, newCachedThreadPool(), 10 * 1024,
				Paths.get(src));
		client.upload(dst, producer, callback);
	}
}
