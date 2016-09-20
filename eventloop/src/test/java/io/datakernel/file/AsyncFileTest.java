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

package io.datakernel.file;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
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
import java.util.concurrent.Executors;

import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

public class AsyncFileTest {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testReadFully() throws Exception {
		final File tempFile = temporaryFolder.newFile("hello-2.html");
		final Eventloop eventloop = Eventloop.create();
		final Path srcPath = Paths.get("test_data/hello.html");
		AsyncFile.open(eventloop, Executors.newCachedThreadPool(), srcPath, new OpenOption[]{READ}, new ResultCallback<AsyncFile>() {
			@Override
			public void onResult(AsyncFile result) {
				logger.info("Opened file.");
				result.readFully(new ResultCallback<ByteBuf>() {
					@Override
					public void onResult(final ByteBuf result) {
						final Path destPath = Paths.get(tempFile.getAbsolutePath());
						AsyncFile.open(eventloop, Executors.newCachedThreadPool(), destPath, new OpenOption[]{WRITE}, new ResultCallback<AsyncFile>() {
							@Override
							public void onResult(AsyncFile file) {
								logger.info("Finished reading file.");

								file.writeFully(result, 0, new CompletionCallback() {
									@Override
									public void onComplete() {
										logger.info("Finished writing file");
										try {
											assertArrayEquals(Files.readAllBytes(srcPath), Files.readAllBytes(destPath));
										} catch (IOException e) {
											logger.info("Could not compare files {} and {}", srcPath, destPath);
											throw new RuntimeException(e);
										}
									}

									@Override
									public void onException(Exception exception) {
										logger.info("Exception thrown while trying to read file.", exception);
									}
								});
							}

							@Override
							public void onException(Exception exception) {
								logger.info("Exception thrown while trying to open file for writing.", exception);
							}
						});

					}

					@Override
					public void onException(Exception exception) {
						logger.info("Exception thrown while trying to read file.", exception);
					}
				});
			}

			@Override
			public void onException(Exception exception) {
				logger.info("Exception thrown while trying to open file for reading.", exception);
			}
		});

		eventloop.run();
		assertThat(eventloop, doesntHaveFatals());
	}
}