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

package io.datakernel.simplefs.stress;

import com.google.common.collect.Lists;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.remotefs.FileSystem;
import io.datakernel.remotefs.FileSystemImpl;
import io.datakernel.remotefs.FsServer;
import io.datakernel.remotefs.RfsConfig;
import io.datakernel.remotefs.protocol.ServerProtocol;
import io.datakernel.remotefs.protocol.gson.GsonServerProtocol;
import io.datakernel.simplefs.SimpleFsServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class StressServer {
	private static final Path SERVER_STORAGE_PATH = Paths.get("./test/server_storage");
	public static final int PORT = 5560;

	private static final ExecutorService executor = newCachedThreadPool();
	private static final NioEventloop eventloop = new NioEventloop();

	public static RfsConfig config = RfsConfig.getDefaultConfig();
	public static FileSystem fileSystem = FileSystemImpl.createInstance(eventloop, executor, SERVER_STORAGE_PATH, config);
	public static ServerProtocol protocol = GsonServerProtocol.createInstance(eventloop, Lists.newArrayList(new InetSocketAddress(PORT)), config);

	public static FsServer server = SimpleFsServer.createInstance(eventloop, fileSystem, protocol, config);

	public static void main(String[] args) throws IOException {
		Files.createDirectories(SERVER_STORAGE_PATH);
		server.start(ignoreCompletionCallback());
		eventloop.run();
	}
}
