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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.Tuple3;
import io.global.fs.api.CheckpointPosStrategy;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.text.DecimalFormat;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static io.global.fs.cli.GlobalFs.err;
import static io.global.fs.cli.GlobalFs.info;

@Command(name = "list", description = "List files from Global-FS namespace")
public final class GlobalFsList implements Callable<Void> {

	@Mixin
	private GlobalFsCommon common;

	@Parameters(index = "2", arity = "0..1", paramLabel = "<glob>", description = "Glob to filter listed files")
	private String glob;

	@Option(names = {"-H", "--machine-readable"}, description = "Output exact file sizes in bytes")
	private boolean machineReadable;

	@Option(names = {"-t", "--show-tombstones"}, description = "Include tombstones in the output")
	private boolean showTombstones;

	@Option(names = {"-T", "--only-tombstones"}, description = "List only the tombstones")
	private boolean onlyTombstones;

	@Override
	public Void call() {
		Tuple3<ExecutorService, Eventloop, FsClient> tuple = common.init(CheckpointPosStrategy.of(8096)); // cps in not used for list

		ExecutorService executor = tuple.getValue1();
		Eventloop eventloop = tuple.getValue2();
		FsClient gateway = tuple.getValue3();

		info("Listing files " + (glob != null ? "with glob '" + glob + "' " : "") + "...");
		gateway.listEntities(glob != null ? glob : "**")
				.whenComplete((list, e) -> {
					if (e != null) {
						err("List " + (glob != null ? '\'' + glob + "' " : "") + "finished with exception " + e);
						return;
					}
					list.forEach(onlyTombstones ?
							showTombstones ?
									(meta -> {
										if (meta.getSize() == -1) {
											System.out.println(meta.getName() + "\t<deleted>");
										}
									}) :
									(meta -> {
										if (meta.getSize() == -1) {
											System.out.println(meta.getName());
										}
									}) :
							showTombstones ?
									(meta -> System.out.println(meta.getName() + '\t' + formatSize(meta.getSize()))) :
									(meta -> {
										long size = meta.getSize();
										if (size != -1) {
											System.out.println(meta.getName() + '\t' + formatSize(size));
										}
									}));
				});

		eventloop.run();
		executor.shutdown();
		return null;
	}

	private static final String[] POWERS = {"b", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
	private static final DecimalFormat FORMAT = new DecimalFormat("#.##");

	private String formatSize(long bytes) {
		if (bytes == -1) {
			return "<deleted>";
		}
		if (machineReadable) {
			return "" + bytes;
		}
		int power = 0;
		double fract = bytes;
		while (power < POWERS.length) {
			if (fract < 1024) {
				return FORMAT.format(fract) + POWERS[power];
			}
			fract /= 1024;
			power++;
		}
		return "<overflow>";
	}
}
