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
import io.datakernel.hashfs.ServerInfo;

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

	public static void main(String[] args) throws IOException {

		// Specifying bootstrap servers - these are considered to be always alive and keep the source info about alive servers map
		ServerInfo server0 = new ServerInfo(0, new InetSocketAddress("127.0.0.1", 5570), 1.0);
		ServerInfo server1 = new ServerInfo(1, new InetSocketAddress("127.0.0.1", 5571), 0.2);
		ServerInfo server2 = new ServerInfo(2, new InetSocketAddress("127.0.0.1", 5572), 1.3);
		ServerInfo server3 = new ServerInfo(3, new InetSocketAddress("127.0.0.1", 5573), 2.6);
		ServerInfo server4 = new ServerInfo(4, new InetSocketAddress("127.0.0.1", 5574), 8.0);
		final Set<ServerInfo> bootstrap = new HashSet<>(Arrays.asList(server0, server1, server2, server3, server4));

		// Core component
		NioEventloop eventloop = new NioEventloop();

		// Starting servers
		for (ServerInfo server : bootstrap) {
			Path serverStorage = Paths.get("./test/server_storage_" + server.getServerId());
			HashFsServer.buildInstance(eventloop, newCachedThreadPool(), serverStorage, server, bootstrap)
					.build()
					.start(ignoreCompletionCallback());
		}
		eventloop.run();
	}
}
