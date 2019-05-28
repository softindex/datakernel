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

package io.datakernel.examples;

import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.di.Inject;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;

/**
 * This example demonstrates uploading file to server using RemoteFS
 * To run this example you should first launch ServerSetupExample
 */
public class FileUploadExample extends Launcher {
	private static final Logger logger = LoggerFactory.getLogger(FileUploadExample.class);

	private static final int SERVER_PORT = 6732;
	private static final Path CLIENT_STORAGE = Paths.get("src/main/resources/client_storage");

	private static final String FILE_NAME = "example.txt";

	@Inject
	private RemoteFsClient client;

	@Inject
	private ExecutorService executor;

	@Inject
	private Eventloop eventloop;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				new AbstractModule() {
					@Provides
					Eventloop eventloop() {
						return Eventloop.create()
								.withFatalErrorHandler(rethrowOnAnyError())
								.withCurrentThread();
					}

					@Provides
					RemoteFsClient remoteFsClient(Eventloop eventloop) {
						return RemoteFsClient.create(eventloop, new InetSocketAddress(SERVER_PORT));
					}

					@Provides
					ExecutorService executor() {
						return Executors.newCachedThreadPool();
					}
				}
		);
	}

	@Override
	protected void run() throws Exception {
		eventloop.post(() -> {

			// consumer result here is a marker of it being successfully uploaded
			ChannelFileReader.readFile(CLIENT_STORAGE.resolve(FILE_NAME))
					.then(cfr -> cfr.withBufferSize(MemSize.kilobytes(16)).streamTo(client.upload(FILE_NAME)))
					.whenComplete(($, e) -> {
						if (e != null) {
							logger.error("Error while uploading file {}", FILE_NAME, e);
						} else {
							logger.info("Client uploaded file {}", FILE_NAME);
						}
						shutdown();
					});

		});
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		FileUploadExample example = new FileUploadExample();
		example.launch(args);
	}
}
