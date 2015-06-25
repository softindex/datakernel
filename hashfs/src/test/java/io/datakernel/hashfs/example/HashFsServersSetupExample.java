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
import io.datakernel.hashfs.protocol.HashFsClientProtocol;
import io.datakernel.hashfs.protocol.gson.HashFsGsonClientProtocol;
import io.datakernel.hashfs.protocol.gson.HashFsGsonServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * To start using HashFS, we have to setup our servers first.
 * In this example, 10 servers, which store their files in ./test/ directory, are started at ports 6732 through 6741
 * (you may change this parameters that are defined as constants in the code below).
 * After successfully running main(), you may proceed to file upload example.
 */
public class HashFsServersSetupExample {
	private static final int FIRST_SERVER_PORT = 6732;
	private static final int SERVER_COUNT = 10;
	private static final Path SERVER_STORAGE_PATH = Paths.get("./test/");

	private static final Logger logger = LoggerFactory.getLogger(HashFsServersSetupExample.class);

	public static void main(String[] args) throws IOException {
		Files.createDirectories(SERVER_STORAGE_PATH);

		final List<ServerInfo> serverInfos = new ArrayList<>();

		// Each server must know port and server id of other servers.
		for (int i = 0; i < SERVER_COUNT; i++) {
			ServerInfo info = new ServerInfo(new InetSocketAddress("127.0.0.1", FIRST_SERVER_PORT + i), i, 1);
			serverInfos.add(info);
		}

		for (final ServerInfo serverInfo : serverInfos) {
			// Create and start each server in a separate thread.
			new Thread(new Runnable() {
				@Override
				public void run() {
					NioEventloop eventloop = new NioEventloop();
					final ExecutorService executor = Executors.newCachedThreadPool();
					HashFsClientProtocol protocol = new HashFsGsonClientProtocol(eventloop, serverInfos);

					HashFsServer server = new HashFsServer(eventloop, serverInfo, serverInfos,
							SERVER_STORAGE_PATH.resolve("server_storage_" + serverInfo.serverId), protocol, executor);
					try {
						HashFsGsonServer.createServerTransport(eventloop, serverInfo, server);
					} catch (IOException e) {
						logger.error("Can't start server with id {}", serverInfo.serverId, e);
					}
					eventloop.run();
				}
			}).start();
		}
	}
}
