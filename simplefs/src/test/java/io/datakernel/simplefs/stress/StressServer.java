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

import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
import io.datakernel.simplefs.SimpleFsServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class StressServer {
	private static final Path STORAGE_PATH = Paths.get("./test/server_storage");
	public static final int PORT = 5560;

	private static final ExecutorService executor = newCachedThreadPool();
	private static final NioEventloop eventloop = new NioEventloop();

	public static NioService server = SimpleFsServer.buildInstance(eventloop, executor, STORAGE_PATH)
			.specifyListenPort(PORT)
			.build();

	public static void main(String[] args) throws IOException {
		Files.createDirectories(STORAGE_PATH);
		server.start(ignoreCompletionCallback());
		eventloop.run();
	}
}
