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

import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.HashFsServer;
import io.datakernel.remotefs.RfsConfig;
import io.datakernel.remotefs.ServerInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class ServerSetupExample {

	private static RfsConfig config;

	public static void main(String[] args) throws IOException {

		ServerInfo server0 = new ServerInfo(0, new InetSocketAddress("127.0.0.1", 5570), 1);
		ServerInfo server1 = new ServerInfo(1, new InetSocketAddress("127.0.0.1", 5571), 1);
		ServerInfo server2 = new ServerInfo(2, new InetSocketAddress("127.0.0.1", 5572), 1);
		ServerInfo server3 = new ServerInfo(3, new InetSocketAddress("127.0.0.1", 5573), 1);
		ServerInfo server4 = new ServerInfo(4, new InetSocketAddress("127.0.0.1", 5574), 1);

		final Set<ServerInfo> bootstrap = new HashSet<>(Arrays.asList(server0, server1, server2, server3, server4));

		NioEventloop eventloop = new NioEventloop();

		for (ServerInfo server : bootstrap) {
			Path serverStorage = Paths.get("./test/server_storage_" + server.getServerId());
			config = RfsConfig.getDefaultConfig();
			HashFsServer.createInstance(eventloop, newCachedThreadPool(), serverStorage, server, bootstrap, config)
					.start(ignoreCompletionCallback());
		}
		eventloop.run();
	}
}
