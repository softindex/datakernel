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

package io.datakernel.simplefs.example;

import io.datakernel.eventloop.NioEventloop;
import io.datakernel.simplefs.SimpleFsServer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * To start using SimpleFS, we have to setup our server first.
 * In this example file server, which stores its files in `./test/server_storage/`, is started at port 6732 (this settings may be changed in constants below).
 * Upon successful completion of main() method, you may proceed to file upload example.
 */
public class SimpleFsServersSetupExample {
	private static final int SERVER_PORT = 6732;
	private static final Path SERVER_STORAGE_PATH = Paths.get("./test/server_storage");

	public static void main(String[] args) throws IOException {
		final ExecutorService executor = Executors.newCachedThreadPool();

		final NioEventloop eventloop = new NioEventloop();

		// Create server
		SimpleFsServer fileServer = SimpleFsServer.createServer(eventloop, SERVER_STORAGE_PATH, executor);

		// Start listening
		fileServer.setListenPort(SERVER_PORT);
		fileServer.listen();

		eventloop.run();
	}
}
