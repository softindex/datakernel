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

import io.datakernel.util.MemSize;
import io.datakernel.util.StringFormatUtils;
import io.datakernel.util.Utils;
import io.global.common.PrivKey;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.DefaultExceptionHandler;
import picocli.CommandLine.Option;
import picocli.CommandLine.RunLast;

import java.net.InetSocketAddress;
import java.util.List;

@Command(name = "globalfs", description = "Basic command line utility for interacting with Global-FS",
		subcommands = {GlobalFsUpload.class, GlobalFsDownload.class, GlobalFsDelete.class})
public final class GlobalFs {
	static boolean loggingEnabled = true;

	@SuppressWarnings("UseOfSystemOutOrSystemErr")
	static void info(String msg) {
		if (loggingEnabled) {
			System.err.println("\033[36m" + msg + "\033[0m");
		}
	}

	@SuppressWarnings("UseOfSystemOutOrSystemErr")
	static void err(String msg) {
		if (loggingEnabled) {
			System.err.println("\033[95m" + msg + "\033[0m");
		}
	}

	@Option(names = {"-h", "--help"}, usageHelp = true, description = "Displays this help message")
	private boolean helpRequested;

	public static void main(String[] args) {
		new CommandLine(GlobalFs.class)
				.registerConverter(InetSocketAddress.class, Utils::parseInetSocketAddress)
				.registerConverter(PrivKey.class, PrivKey::fromString)
				.registerConverter(MemSize.class, StringFormatUtils::parseMemSize)

				.parseWithHandlers(new RunLast(), new DefaultExceptionHandler<List<Object>>().andExit(1), args.length == 0 ? new String[]{"-h"} : args);
	}
}
