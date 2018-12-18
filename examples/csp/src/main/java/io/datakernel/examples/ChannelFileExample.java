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

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.eventloop.Eventloop;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ChannelFileExample {
	private static final ExecutorService executorService = Executors.newCachedThreadPool();
	private static final Path PATH = Paths.get("src/main/resources/NewFile.txt");
	private static final Eventloop eventloop = Eventloop.create().withCurrentThread();

	private static Promise<Void> writeToFile() {
		try {
			return ChannelSupplier.of(
					ByteBufStrings.wrapAscii("Hello, this is example file\n"),
					ByteBufStrings.wrapAscii("This is the second line of file"))
					.streamTo(ChannelFileWriter.create(executorService, PATH));
		} catch (IOException e) {
			return Promise.ofException(e);
		}
	}

	private static Promise<Void> readFile() {
		try {
			return ChannelFileReader.readFile(executorService, PATH)
					.streamTo(ChannelConsumer.ofConsumer(buf -> System.out.println(buf.asString(UTF_8))));
		} catch (IOException e) {
			return Promise.ofException(e);
		}
	}

	private static void cleanUp() {
		try {
			Files.delete(PATH);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			executorService.shutdown();
		}
	}

	public static void main(String[] args) {
		Promises.runSequence(
				AsyncSupplier.cast(ChannelFileExample::writeToFile),
				AsyncSupplier.cast(ChannelFileExample::readFile))
				.whenComplete(($, e) -> {
					if (e != null) {
						e.printStackTrace();
					}
					cleanUp();
				});

		eventloop.run();
	}
}
