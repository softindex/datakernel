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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.MemSize;
import io.datakernel.common.tuple.Tuple3;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.remotefs.FsClient;
import io.global.fs.api.CheckpointPosStrategy;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static io.global.fs.cli.GlobalFs.err;
import static io.global.fs.cli.GlobalFs.info;
import static picocli.CommandLine.Help.Visibility.ALWAYS;

@Command(name = "upload", description = "Upload file to the Global-FS")
public final class GlobalFsUpload implements Callable<Void> {

	@Mixin
	private GlobalFsCommon common;

	@Parameters(index = "2", arity = "0..1", paramLabel = "<file>",
			description = "File to upload. Can be empty for reading from stdin, but the -r option will be required")
	private String file;

	@Option(names = {"-c", "--checkpoint-interval"}, paramLabel = "<checkpoint interval>", defaultValue = "8kb", showDefaultValue = ALWAYS,
			description = "Number of bytes between checkpoints. Allows suffixes")
	private MemSize checkpointInterval;

	@Option(names = {"--offset"}, defaultValue = "0", description = "Offset in bytes from which to start uploading the file. Allows suffixes.")
	private long offset;

	@Option(names = {"-n", "--name"}, paramLabel = "<name>", description = "How to name the file in Global-FS")
	private String remoteName;

	@Option(names = {"-r", "--revision"}, paramLabel = "<revision>", description = "Set custom revision to try to upload")
	private long revision;

	private final Executor executor;

	public GlobalFsUpload(Executor executor) {
		this.executor = executor;
	}

	@Override
	public Void call() throws Exception {
		Tuple3<ExecutorService, Eventloop, FsClient> tuple = common.init(CheckpointPosStrategy.of(checkpointInterval.toLong()));

		ExecutorService executor = tuple.getValue1();
		Eventloop eventloop = tuple.getValue2();
		FsClient gateway = tuple.getValue3();

		String name;
		Promise<? extends ChannelSupplier<ByteBuf>> reader;

		if (file == null) {
			if (remoteName == null) {
				err("When uploading from standard input, the --remote-name option is required");
				return null;
			}
			name = remoteName;

			reader = Promise.of(ChannelSupplier.of(() ->
					Promise.ofBlockingCallable(this.executor, () -> {
						ByteBuf buffer = ByteBufPool.allocate(4096);
						int bytes = System.in.read(buffer.array());
						if (bytes == -1) {
							buffer.recycle();
							return null;
						}
						buffer.moveTail(bytes);
						return buffer;
					})));
			info("Uploading data from standard input as " + name + " ...");
		} else {
			Path path = Paths.get(file);
			reader = ChannelFileReader.open(this.executor, path);
			if (remoteName == null) {
				name = path.getFileName().toString();
				info("Uploading " + name + " ...");
			} else {
				name = remoteName;
				info("Uploading " + path.getFileName() + " as " + name + " ...");
			}
		}

		reader.whenResult(channel -> channel.streamTo(gateway.upload(name, offset, revision))
				.whenComplete(($, e) -> {
					if (e == null) {
						info(name + " upload finished");
						return;
					}
					err("Upload '" + name + "' finished with exception " + e);
				}));

		eventloop.run();
		executor.shutdown();
		return null;
	}
}
