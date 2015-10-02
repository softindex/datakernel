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

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.simplefs.SimpleFsServer;
import io.datakernel.simplefs.StopAndHugeFileUploadTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StressServer {
	private static final Logger logger = LoggerFactory.getLogger(StopAndHugeFileUploadTest.class);

	private static final int SERVER_PORT = 6732;

	// Specify path. Should be manually deleted after server stop
	private static final String tmpPath = "";
	private static final Path SERVER_STORAGE_PATH = Paths.get(tmpPath);

	private static final ExecutorService executor = Executors.newCachedThreadPool();
	private static final NioEventloop eventloop = new NioEventloop();

	public static SimpleFsServer fileServer = SimpleFsServer.createServer(eventloop, SERVER_STORAGE_PATH, executor);

	public static void main(String[] args) throws IOException {
		Files.createDirectories(SERVER_STORAGE_PATH);

		fileServer.setListenPort(SERVER_PORT);
		fileServer.listen();
		fileServer.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Started");
			}

			@Override
			public void onException(Exception e) {
				logger.error(e.getMessage());
			}
		});

		eventloop.run();
	}
}
