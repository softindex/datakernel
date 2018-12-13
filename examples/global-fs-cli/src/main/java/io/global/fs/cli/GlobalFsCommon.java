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
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.Tuple3;
import io.global.common.PrivKey;
import io.global.common.PrivateKeyStorage;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.GlobalFsDriver;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.util.CollectionUtils.map;

@Command
public final class GlobalFsCommon {

	@Parameters(index = "0", description = "Global-FS endpoint URL")
	private URL endpoint;

	@Parameters(index = "1", paramLabel = "<private key>", description = "Private key of the namespace to work with")
	private PrivKey privKey;

	@Option(names = {"-q", "--quiet"}, description = "Suppresses all informational output to standard error stream")
	private boolean quiet;

	@Option(names = {"-h", "--help"}, usageHelp = true, description = "Displays this help message")
	private boolean helpRequested;

	public Tuple3<ExecutorService, Eventloop, FsClient> init(CheckpointPosStrategy checkpointPosStrategy) {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Eventloop eventloop = Eventloop.create()
				.withCurrentThread()
				.withFatalErrorHandler(rethrowOnAnyError());

		if (quiet) {
			GlobalFs.loggingEnabled = false;
		}

		GlobalFsNode node = HttpGlobalFsNode.create(endpoint.toString(), AsyncHttpClient.create(eventloop));
		PrivateKeyStorage pks = new PrivateKeyStorage(map(privKey.computePubKey(), privKey));

		return new Tuple3<>(executor, eventloop, GlobalFsDriver.create(node, pks, checkpointPosStrategy).gatewayFor(privKey.computePubKey()));
	}
}
