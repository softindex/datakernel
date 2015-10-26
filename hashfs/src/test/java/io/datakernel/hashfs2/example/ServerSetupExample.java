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
import io.datakernel.hashfs2.net.Protocol;
import io.datakernel.hashfs2.net.gson.ProtocolImpl;
import io.datakernel.hashfs2.net.gson.TransportImp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class ServerSetupExample {
	public static void main(String[] args) throws IOException {
		ServerInfo myId = new ServerInfo(0, new InetSocketAddress("127.0.0.1", 5577), 1);
		NioEventloop eventloop = new NioEventloop();

		Protocol protocol = new ProtocolImpl(eventloop, 10 * 256);

		FileSystem fileSystem = FileSystemImpl.init(eventloop, newCachedThreadPool(), Paths.get("./test/server_storage_0"), 10 * 256);

		HashFS server = new HashFS(eventloop, fileSystem, protocol);

		LogicImpl logic = new LogicImpl(new RendezvousHashing(), myId, server);
		server.wirelogic(logic);
		logic.init(new HashSet<>(Collections.singletonList(myId)));
		AbstractNioServer transport = new TransportImp(eventloop, server);

		transport.setListenPort(5577);
		transport.listen();
		eventloop.run();
	}
}
