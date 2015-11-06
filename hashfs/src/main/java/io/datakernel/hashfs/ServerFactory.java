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

package io.datakernel.hashfs;

import com.google.common.collect.Lists;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
import io.datakernel.hashfs.protocol.ClientProtocol;
import io.datakernel.hashfs.protocol.ServerProtocol;
import io.datakernel.hashfs.protocol.gson.GsonClientProtocol;
import io.datakernel.hashfs.protocol.gson.GsonServerProtocol;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class ServerFactory {

	public static NioService getServer(NioEventloop eventloop, ExecutorService executor, Path storagePath,
	                                   Config config, ServerInfo myId, Set<ServerInfo> bootstrap) {
		ClientProtocol protocol = createGsonClientProtocol(eventloop, config);
		FileSystem fileSystem = createFileSystem(eventloop, executor, storagePath, config);
		HashFsNode node = new HashFsNode(eventloop, fileSystem, protocol, config.getSystemUpdateTimeout(), config.getMapUpdateTimeout());
		HashingStrategy hashing = createHashingStrategy();
		Logic logic = createLogic(node, hashing, myId, bootstrap, config);
		ServerProtocol transport = getServerProtocol(eventloop, node, config, myId.getAddress().getPort());
		node.wire(logic, transport);
		return node;
	}

	public static ServerProtocol getServerProtocol(NioEventloop eventloop, Server server, Config config, int port) {
		GsonServerProtocol serverProtocol = new GsonServerProtocol(eventloop, server, config.getDeserializerBufferSize(), config.getSerializerBufferSize(),
				config.getSerializerMaxMessageSize(), config.getSerializerFlushDelayMillis());
		serverProtocol.setListenPort(port);
		return serverProtocol;
	}

	public static FsClient getClient(NioEventloop eventloop, Set<ServerInfo> bootstrap, Config config) {
		ClientProtocol protocol = createGsonClientProtocol(eventloop, config);
		HashingStrategy hashing = createHashingStrategy();
		return new HashFsClient(eventloop, protocol, hashing, Lists.newArrayList(bootstrap),
				config.getBaseRetryTimeout(), config.getMaxRetryAttempts());
	}

	public static HashingStrategy createHashingStrategy() {return new RendezvousHashing();}

	public static ClientProtocol createGsonClientProtocol(NioEventloop eventloop, Config c) {
		return new GsonClientProtocol(eventloop, c.getMinChunkSize(), c.getMaxChunkSize(),
				c.getDeserializerBufferSize(), c.getConnectTimeout(), c.getSerializerBufferSize(),
				c.getSerializerMaxMessageSize(), c.getSerializerFlushDelayMillis(),
				c.getSocketSettings());
	}

	public static FileSystem createFileSystem(NioEventloop eventloop, ExecutorService executor,
	                                          Path storagePath, Config config) {
		Path tmpStorage = storagePath.resolve(config.getTmpDirectoryName());

		return new FileSystemImpl(eventloop, executor, storagePath, tmpStorage,
				config.getFsReaderBufferSize(), config.getInProgressExtension());
	}

	public static Logic createLogic(Commands commands, HashingStrategy hashing, ServerInfo myId, Set<ServerInfo> bootstrap, Config c) {
		return new LogicImpl(commands, hashing, myId, bootstrap, c.getServerDeathTimeout(),
				c.getMaxReplicaQuantity(), c.getMinSafeReplicasQuantity(), c.getApproveWaitTime());
	}
}