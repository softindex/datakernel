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

package io.datakernel.hashfs2.example;

import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs2.Config;
import io.datakernel.hashfs2.FsClient;
import io.datakernel.hashfs2.ServerFactory;
import io.datakernel.hashfs2.ServerInfo;
import io.datakernel.stream.StreamProducer;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static io.datakernel.stream.file.StreamFileReader.readFileFully;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class FileUploadExample {
	public static void main(String[] args) {
		NioEventloop eventloop = new NioEventloop();
		ServerInfo server3 = new ServerInfo(3, new InetSocketAddress("127.0.0.1", 5573), 1);
		ServerInfo server4 = new ServerInfo(4, new InetSocketAddress("127.0.0.1", 5574), 1);

		final Set<ServerInfo> bootstrap = new HashSet<>(Arrays.asList(server3, server4));

		FsClient client = ServerFactory.getClient(eventloop, bootstrap, Config.getDefaultConfig());
		StreamProducer<ByteBuf> producer = readFileFully(eventloop, newCachedThreadPool(), 10 * 1024, Paths.get("./test/client_storage/rejected.txt"));

		client.upload("test.txt", producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				System.out.println("Success");
			}

			@Override
			public void onException(Exception e) {
				System.out.println("Failed");
			}
		});

		eventloop.run();
	}
}
