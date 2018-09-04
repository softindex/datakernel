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

package io.datakernel.remotefs.stress;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.serial.file.SerialFileWriter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

public class StressDownload {
	private static final int OPERATIONS_QUANTITY = 10 * 1024;
	private static final int FILE_MAX_SIZE = 1024;

	private static final Path CLIENT_STORAGE = Paths.get("./test_data/client_storage");

	private static Random rand = new Random();
	public static final List<String> FILES = new ArrayList<>();

	public static void main(String[] args) throws IOException {

		Files.createDirectories(CLIENT_STORAGE);

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newCachedThreadPool();

		int[] failures = new int[1];

		RemoteFsClient client = RemoteFsClient.create(eventloop, new InetSocketAddress("localhost", 5560));

		for (int i = 0; i < OPERATIONS_QUANTITY; i++) {
			FILES.add(createFile());
		}

		for (int i = 0; i < OPERATIONS_QUANTITY; i++) {
			String file = FILES.get(rand.nextInt(OPERATIONS_QUANTITY));
			client.download(file, 0).whenComplete((producer, throwable) -> {
				if (throwable == null) {
					try {
						producer.streamTo(SerialFileWriter.create(executor, CLIENT_STORAGE.resolve(file)));
					} catch (IOException e) {
						failures[0]++;
					}
				} else {
					failures[0]++;
				}
			});

			eventloop.run();
		}

		executor.shutdown();
		System.out.println("Failures: " + failures[0]);
	}

	public static String createFile() throws IOException {
		String name = Integer.toString(rand.nextInt(Integer.MAX_VALUE));
		Path file = StressServer.STORAGE_PATH.resolve(name);
		byte[] bytes = new byte[FILE_MAX_SIZE];
		rand.nextBytes(bytes);
		Files.write(file, bytes);
		return name;
	}
}
