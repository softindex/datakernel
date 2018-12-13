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

import io.global.common.PrivKey;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import static io.global.fs.cli.GlobalFs.info;

@Command(name = "alias", description = "Alias a private key with short and easy to remember name\n" +
		"\033[91mBE AWARE THAT ALIAS IS A TEMPORARY HELPER FOR TESTING, THAT STORES PRIVATE KEYS IN AN UNPROTECTED LOCATION\033[0m")
public final class GlobalFsAlias implements Runnable {

	@Parameters(index = "0", description = "A name for the key")
	private String name;

	@Parameters(index = "1", paramLabel = "<private key>", description = "Private key to be aliased")
	private PrivKey privKey;

	@Override
	public void run() {
		String str = privKey.asString();
		GlobalFs.keyStorage.put(name, str);
		info("Added '" + name + "' alias for key");
	}
}
