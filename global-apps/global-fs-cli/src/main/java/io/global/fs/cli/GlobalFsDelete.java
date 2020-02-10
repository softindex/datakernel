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

import io.datakernel.common.tuple.Tuple3;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FsClient;
import io.global.fs.api.CheckpointPosStrategy;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static io.global.fs.cli.GlobalFs.err;
import static io.global.fs.cli.GlobalFs.info;

@Command(name = "delete", description = "Delete file on the Global-FS")
public final class GlobalFsDelete implements Callable<Void> {

	@Mixin
	private GlobalFsCommon common;

	@Parameters(index = "2..*", arity = "1..*", paramLabel = "<file>", description = "File(s) to delete")
	private String[] files;

	@Override
	public Void call() {
		Tuple3<ExecutorService, Eventloop, FsClient> tuple = common.init(CheckpointPosStrategy.of(8096)); // cps in not used for list

		ExecutorService executor = tuple.getValue1();
		Eventloop eventloop = tuple.getValue2();
		FsClient gateway = tuple.getValue3();

		for (String file : files) {
			info("Deleting " + file + " ...");
			gateway.delete(file)
					.whenComplete(($, e) -> {
						if (e == null) {
							info(file + " delete finished");
							return;
						}
						err("Delete '" + file + "' finished with exception " + e);
					});
		}

		eventloop.run();
		executor.shutdown();
		return null;
	}
}
