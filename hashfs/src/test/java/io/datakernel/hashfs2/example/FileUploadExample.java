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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs2.ServerInfo;
import io.datakernel.hashfs2.net.gson.ProtocolImpl;

import java.net.InetSocketAddress;
import java.nio.file.Paths;

import static io.datakernel.stream.file.StreamFileReader.readFileFully;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class FileUploadExample {
	public static void main(String[] args) {
		NioEventloop eventloop = new NioEventloop();
		ProtocolImpl client = new ProtocolImpl(eventloop, 256 * 1024);

		client.upload(new ServerInfo(1, new InetSocketAddress("127.0.0.1", 5577), 1), "egegei.txt",
				readFileFully(eventloop, newCachedThreadPool(), 10 * 256, Paths.get("./test/client_storage/egegei.txt")), new CompletionCallback() {
			@Override
			public void onComplete() {
				System.out.println("Ura!!!");
			}

			@Override
			public void onException(Exception exception) {

			}
		});
		eventloop.run();
	}
}
