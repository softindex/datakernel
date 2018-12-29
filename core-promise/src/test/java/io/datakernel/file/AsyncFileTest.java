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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.stream.processor.Manual;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.Executors;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;

@RunWith(DatakernelRunner.class)
public final class AsyncFileTest {
	private static final Logger logger = LoggerFactory.getLogger(AsyncFileTest.class);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testReadFully() throws Exception {
		File tempFile = temporaryFolder.newFile("hello-2.html");
		Path srcPath = Paths.get("test_data/hello.html");
		AsyncFile srcFile = await(AsyncFile.openAsync(Executors.newCachedThreadPool(), srcPath, new OpenOption[]{READ}));
		logger.info("Opened file.");
		ByteBuf byteBuf = await(srcFile.read());
		Path destPath = Paths.get(tempFile.getAbsolutePath());

		AsyncFile destFile = await(AsyncFile.openAsync(Executors.newCachedThreadPool(), destPath, new OpenOption[]{WRITE}));
		logger.info("Finished reading file.");
		await(destFile.write(byteBuf));
		logger.info("Finished writing file");

		assertArrayEquals(Files.readAllBytes(srcPath), Files.readAllBytes(destPath));
	}

	@Test
	@Manual("This test may fail if read finishes before close, hence big file size")
	public void testClose() throws Exception {
		File file = temporaryFolder.newFile("100Mb");
		byte[] data = new byte[100 * 1024 * 1024]; // the larger the file the less chance that it will be read fully before close completes
		new Random().nextBytes(data);
		Files.write(file.toPath(), data);
		Path srcPath = file.toPath();
		AsyncFile asyncFile = await(AsyncFile.openAsync(Executors.newCachedThreadPool(), srcPath, new OpenOption[]{READ}));
		logger.info("Opened file");
		Eventloop originalEventloop = Eventloop.getCurrentEventloop();

		// closing file from outside
		new Thread(() -> originalEventloop.execute(asyncFile::close)).start();

		Throwable e = awaitException(asyncFile.read());
		assertSame(AsyncFile.FILE_CLOSED, e);
	}
}
