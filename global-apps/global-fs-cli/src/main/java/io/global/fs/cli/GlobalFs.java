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

import io.datakernel.common.MemSize;
import io.datakernel.common.StringFormatUtils;
import io.datakernel.common.Utils;
import io.datakernel.common.parse.ParseException;
import io.global.common.PrivKey;
import picocli.CommandLine;
import picocli.CommandLine.*;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

@Command(name = "globalfs", description = "Basic command line utility for interacting with Global-FS",
		subcommands = {GlobalFsUpload.class, GlobalFsDownload.class, GlobalFsDelete.class, GlobalFsList.class, GlobalFsAlias.class})
public final class GlobalFs {
	static boolean loggingEnabled = true;
	static Preferences keyStorage = Preferences.userRoot().node("globalfs/keys");

	static void info(String msg) {
		if (loggingEnabled) {
			System.err.println("\033[36m" + msg + "\033[0m");
		}
	}

	static void err(String msg) {
		if (loggingEnabled) {
			System.err.println("\033[95m" + msg + "\033[0m");
		}
	}

	@Option(names = {"-h", "--help"}, usageHelp = true, description = "Displays this help message")
	private boolean helpRequested;

	public static void main(String[] args) {
		CommandLine commandLine = new CommandLine(GlobalFs.class);

		commandLine
				.registerConverter(InetSocketAddress.class, Utils::parseInetSocketAddress)
				.registerConverter(PrivKey.class, str -> {
					PrivKey privKey;
					try {
						privKey = PrivKey.fromString(keyStorage.get(str, str));
					} catch (ParseException e) {
						throw new ParameterException(commandLine, "Private key is not a stored alias nor a valid key", e, null, str);
					}
					return privKey;
				})
				.registerConverter(MemSize.class, StringFormatUtils::parseMemSize)
				.parseWithHandlers(new RunLast(), new DefaultExceptionHandler<List<Object>>().andExit(1), args.length == 0 ? new String[]{"-h"} : args);

		try {
			keyStorage.flush();
		} catch (BackingStoreException e) {
			err("Failed flushing the key storage for some reason: " + e);
		}
	}
}
