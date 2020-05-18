/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.remotefs.stress;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.RemoteFsServer;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class StressServer {

	static {
		((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.INFO);
	}

	static final Path STORAGE_PATH = Paths.get("./test_data/server_storage");
	private static final int PORT = getFreePort();

	private static final ExecutorService executor = newCachedThreadPool();
	private static final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

	public static RemoteFsServer server = RemoteFsServer.create(eventloop, executor, STORAGE_PATH)
			.withListenPort(PORT);

	public static void main(String[] args) throws IOException {
		Files.createDirectories(STORAGE_PATH);
		server.listen();
		eventloop.run();
		executor.shutdown();
	}
}
