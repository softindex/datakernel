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

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.UncheckedException;
import io.datakernel.file.AsyncFileService;
import io.datakernel.file.ExecutorAsyncFileService;
import io.datakernel.logger.LoggerConfigurer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.util.CollectionUtils.set;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;

@SuppressWarnings("Convert2MethodRef")
public class AsyncFileServiceExample {
	private static final ExecutorService executorService = Executors.newCachedThreadPool();
	private static final AsyncFileService fileService = new ExecutorAsyncFileService(executorService);
	private static final Path PATH;
	static {
		try {
			PATH = Files.createTempFile("NewFile", "txt");
		} catch (IOException e) {
			throw new UncheckedException(e);
		}

		LoggerConfigurer.enableSLF4Jbridge();
	}

	@NotNull
	private static Promise<Void> writeToFile() {
		try {
			FileChannel channel = FileChannel.open(PATH, set(WRITE, APPEND));

			byte[] message1 = "Hello\n".getBytes();
			byte[] message2 = "This is test file\n".getBytes();
			byte[] message3 = "This is the 3rd line in file".getBytes();

			return fileService.write(channel, 0, message1, 0, message1.length)
					.then($ -> fileService.write(channel, 0, message2, 0, message2.length))
					.then($ -> fileService.write(channel, 0, message3, 0, message3.length))
					.toVoid();
		} catch (IOException e) {
			return Promise.ofException(e);
		}
	}

	@NotNull
	private static Promise<ByteBuf> readFromFile() {
		byte[] array = new byte[1024];
		FileChannel channel;
		try {
			channel = FileChannel.open(PATH, set(READ));
		} catch (IOException e) {
			return Promise.ofException(e);
		}

		return fileService.read(channel, 0, array, 0, array.length)
				.map($ -> {
					ByteBuf buf = ByteBuf.wrapForReading(array);
					System.out.println(buf.getString(UTF_8));
					return buf;
				});
	}

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		Promises.sequence(
				() -> writeToFile(),
				() -> readFromFile().toVoid())
				.whenComplete(($, e) -> {
					if (e != null) {
						System.out.println("Something went wrong : " + e);
					}
					executorService.shutdown();
				});

		eventloop.run();
	}
}
