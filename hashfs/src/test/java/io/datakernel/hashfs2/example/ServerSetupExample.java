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

import io.datakernel.eventloop.AbstractNioServer;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs2.*;
import io.datakernel.hashfs2.Client;
import io.datakernel.hashfs2.protocol.GsonClientProtocol;
import io.datakernel.hashfs2.protocol.GsonServerProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class ServerSetupExample {
	public static void main(String[] args) throws IOException {

		final Set<ServerInfo> servers = new HashSet<>(Arrays.asList(new ServerInfo(0, new InetSocketAddress("127.0.0.1", 5577), 1),
				new ServerInfo(1, new InetSocketAddress("127.0.0.1", 5577 + 1), 1),
				new ServerInfo(2, new InetSocketAddress("127.0.0.1", 5577 + 2), 1),
				new ServerInfo(3, new InetSocketAddress("127.0.0.1", 5577 + 3), 1),
				new ServerInfo(4, new InetSocketAddress("127.0.0.1", 5577 + 4), 1)));

		for (int i = 0; i < 5; i++) {
			final int finalI = i;
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						ServerInfo myId = new ServerInfo(finalI, new InetSocketAddress("127.0.0.1", 5577 + finalI), 1);
						NioEventloop eventloop = new NioEventloop();

						Client client = new GsonClientProtocol(eventloop, 10 * 256);

						FileSystem fileSystem = FileSystemImpl.init(eventloop, newCachedThreadPool(), Paths.get("./test/server_storage_" + finalI), Config.defaultConfig);

						HashFS server = new HashFS(eventloop, fileSystem, client);

						LogicImpl logic = new LogicImpl(new RendezvousHashing(), myId, server);
						server.wireLogic(logic);
						logic.init(servers);
						AbstractNioServer transport = new GsonServerProtocol(eventloop, server);

						transport.setListenPort(5577 + finalI);
						transport.listen();
						eventloop.run();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}).start();
		}
	}
}
