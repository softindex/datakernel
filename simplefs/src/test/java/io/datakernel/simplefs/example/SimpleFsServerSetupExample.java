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

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.simplefs.SimpleFsServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newCachedThreadPool;

/**
 * To start using SimpleFS, we have to setup our server first.
 * In this example file server, which stores its files in current directory, is started at port 6732 (this settings may be changed in constants below).
 * Upon successful completion of main() method, you may proceed to file upload example.
 */
public class SimpleFsServerSetupExample {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsServerSetupExample.class);

	public static final int SERVER_PORT = 6732;

	/*  Configuring directories that would keep files
	 *  Bear in mind that specified directories must not relate somehow
	 *  e.g. parent-child like
	 */
	private static final Path SERVER_STORAGE_PATH = Paths.get("./test_data/storage");
	private static final Path TMP_STORAGE_PATH = Paths.get("./test_data/tmp");

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();

		// Configuring and creating server
		SimpleFsServer fileServer = SimpleFsServer.createInstance(eventloop, executor, SERVER_STORAGE_PATH, TMP_STORAGE_PATH, SERVER_PORT);

		// Starting listening to port
		fileServer.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Server started on localhost:{}", SERVER_PORT);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't start server", e);
			}
		});

		eventloop.run();
		executor.shutdown();
	}
}
