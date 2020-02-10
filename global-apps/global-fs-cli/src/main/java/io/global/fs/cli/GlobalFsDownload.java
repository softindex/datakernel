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

package io.global.fs.cli;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.tuple.Tuple3;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.remotefs.FsClient;
import io.global.fs.api.CheckpointPosStrategy;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static io.datakernel.common.collection.CollectionUtils.set;
import static io.global.fs.cli.GlobalFs.err;
import static io.global.fs.cli.GlobalFs.info;
import static java.nio.file.StandardOpenOption.*;

@Command(name = "download", description = "Download file from Global-FS")
public final class GlobalFsDownload implements Callable<Void> {
	private static final Set<OpenOption> OPEN_OPTIONS = set(WRITE, CREATE, TRUNCATE_EXISTING);
	private final Executor executor;
	@Mixin
	private GlobalFsCommon common;

	@Parameters(index = "2", paramLabel = "<file>", description = "File to download")
	private String file;

	@Option(names = {"--offset"}, defaultValue = "0", description = "Offset in bytes from which to start downloading the file. Allows suffixes.")
	private long offset;

	@Option(names = {"--length"}, defaultValue = "-1", description = "Number of bytes to download from file. Allows suffixes.")
	private long length;

	@Option(names = {"-o", "--output-file"}, paramLabel = "<output file>", description = "Where to put and how to name the file when downloading. Allows '-' for STDOUT")
	private String localFile;

	public GlobalFsDownload(Executor executor) {
		this.executor = executor;
	}

	@Override
	public Void call() {
		Tuple3<ExecutorService, Eventloop, FsClient> tuple = common.init(CheckpointPosStrategy.of(8096)); // cps in not used for list

		ExecutorService executor = tuple.getValue1();
		Eventloop eventloop = tuple.getValue2();
		FsClient gateway = tuple.getValue3();

		Promise<ChannelConsumer<ByteBuf>> writer;
		if (localFile == null) {
			info("Downloading " + file + " ...");
			writer = Promise.ofBlockingCallable(this.executor, () -> {
				FileChannel fileChannel = FileChannel.open(Paths.get(localFile), OPEN_OPTIONS);
				return ChannelFileWriter.create(this.executor, fileChannel);
			});
		} else if (localFile.equals("-")) {
			info("Downloading " + file + " to standard output ...");
			writer = Promise.of(ChannelConsumer.of(buffer ->
					Promise.ofBlockingRunnable(this.executor, () ->
							System.out.write(buffer.array(), buffer.head(), buffer.readRemaining()))));
		} else {
			info("Downloading " + file + " as " + localFile + " ...");
			writer = Promise.ofBlockingCallable(this.executor, () -> {
				FileChannel fileChannel = FileChannel.open(Paths.get(localFile), OPEN_OPTIONS);
				return ChannelFileWriter.create(this.executor, fileChannel);
			});
		}

		ChannelSupplier.ofPromise(gateway.download(file, offset, length))
				.streamTo(writer)
				.whenComplete(($, e) -> {
					if (e == null) {
						info(file + " download finished");
						return;
					}
					err("Download '" + file + "' finished with exception " + e);
				});

		eventloop.run();
		executor.shutdown();
		return null;
	}
}
