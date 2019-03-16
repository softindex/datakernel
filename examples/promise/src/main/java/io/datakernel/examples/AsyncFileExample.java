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
import io.datakernel.file.AsyncFile;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.util.CollectionUtils.set;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;

@SuppressWarnings("Convert2MethodRef")
public class AsyncFileExample {
	private static final ExecutorService executorService = Executors.newCachedThreadPool();
	private static final Path PATH = Paths.get("src/main/resources/NewFile.txt");
	private static final Eventloop eventloop = Eventloop.create().withCurrentThread();

	@NotNull
	private static Promise<Void> writeToFile() {
		try {
			AsyncFile asyncFile = AsyncFile.open(executorService, PATH, set(WRITE, CREATE_NEW, APPEND));
			return asyncFile.write(wrapAscii("Hello\n"))
					.then($ -> asyncFile.write(wrapAscii("This is test file\n")))
					.then($ -> asyncFile.write(wrapAscii("This is the 3rd line in file")));
		} catch (IOException e) {
			return Promise.ofException(e);
		}
	}

	@NotNull
	private static Promise<ByteBuf> readFromFile() {
		try {
			return AsyncFile.open(executorService, PATH, set(READ))
					.read()
					.accept(buf -> System.out.println(buf.getString(UTF_8)));
		} catch (IOException e) {
			return Promise.ofException(e);
		}
	}

	private static void cleanup() {
		try {
			Files.delete(PATH);
		} catch (IOException e) {
			System.out.println("Failed to delete file (" + PATH + "): " + e);
		} finally {
			executorService.shutdown();
		}
	}

	public static void main(String[] args) {
		Promises.sequence(
				() -> writeToFile(),
				() -> readFromFile().toVoid())
				.acceptEx(($, e) -> {
					if (e != null) {
						System.out.println("Something went wrong : " + e);
					}
					cleanup();
				});

		eventloop.run();
	}
}
