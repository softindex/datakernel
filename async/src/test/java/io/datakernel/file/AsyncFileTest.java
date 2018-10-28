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

package io.datakernel.file;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.ActivePromisesRule;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;

public class AsyncFileTest {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Rule
	public ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testReadFully() throws Exception {
		File tempFile = temporaryFolder.newFile("hello-2.html");
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		Path srcPath = Paths.get("test_data/hello.html");
		AsyncFile.openAsync(Executors.newCachedThreadPool(), srcPath, new OpenOption[]{READ}).whenComplete(assertComplete(asyncFile -> {
			logger.info("Opened file.");
			asyncFile.read().whenComplete(assertComplete(byteBuf -> {
				Path destPath = Paths.get(tempFile.getAbsolutePath());

				AsyncFile.openAsync(Executors.newCachedThreadPool(), destPath, new OpenOption[]{WRITE}).whenComplete(assertComplete(file -> {
					logger.info("Finished reading file.");

					file.write(byteBuf).whenComplete(assertComplete($ -> {
						logger.info("Finished writing file");
						try {
							assertArrayEquals(Files.readAllBytes(srcPath), Files.readAllBytes(destPath));
						} catch (IOException e) {
							logger.info("Could not compare files {} and {}", srcPath, destPath);
							throw new RuntimeException(e);
						}
					}));
				}));
			}));
		}));

		eventloop.run();
	}

	@Test
	public void testClose() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		File file = temporaryFolder.newFile("10Mb");
		byte[] data = new byte[10 * 1024 * 1024]; // the larger the file the less chance that it will be read fully before close completes
		new Random().nextBytes(data);
		Files.write(file.toPath(), data);
		Path srcPath = file.toPath();
		AsyncFile.openAsync(Executors.newCachedThreadPool(), srcPath, new OpenOption[]{READ})
				.whenComplete(assertComplete(asyncFile -> {
					logger.info("Opened file");
					asyncFile.read().whenComplete((res, e) -> {
						if (e != null) {
							assertSame(AsyncFile.FILE_CLOSED, e);
						} else {
							// rare cases when read finishes before close
							logger.info("Read has finished prior to close");
							assertArrayEquals(data, res.asArray());
						}
					});
					logger.info("Calling close file");
					asyncFile.close()
							.whenComplete(assertComplete($ -> logger.info("Closed file")));
				}));

		eventloop.run();
	}
}
