/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.logfs;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

public final class RemoteLogFileSystem extends AbstractLogFileSystem {
	private static final String LOG_NAME_DELIMITER = "/";

	private final Eventloop eventloop;
	private final String logName;
	private final RemoteFsClient client;

	private RemoteLogFileSystem(Eventloop eventloop, String logName, RemoteFsClient client) {
		this.eventloop = eventloop;
		this.logName = logName;
		this.client = client;
	}

	public static RemoteLogFileSystem create(Eventloop eventloop, String logName, RemoteFsClient client) {
		return new RemoteLogFileSystem(eventloop, logName, client);
	}

	@Override
	public CompletionStage<List<LogFile>> list(String logPartition) {
		return client.list().thenApply(files -> getLogFiles(files, logPartition));
	}

	@Override
	public CompletionStage<StreamProducerWithResult<ByteBuf, Void>> read(String logPartition, LogFile logFile, long startPosition) {
		return client.download(path(logPartition, logFile), startPosition);
	}

	@Override
	public CompletionStage<StreamConsumerWithResult<ByteBuf, Void>> write(String logPartition, LogFile logFile) {
		final String fileName = path(logPartition, logFile);
		return client.upload(fileName);
	}

	private String path(String logPartition, LogFile logFile) {
		return logName + LOG_NAME_DELIMITER + fileName(logPartition, logFile);
	}

	private List<LogFile> getLogFiles(List<String> fileNames, String logPartition) {
		List<LogFile> entries = new ArrayList<>();
		for (String file : fileNames) {
			String[] splittedFileName = file.split(LOG_NAME_DELIMITER);
			String fileLogName = splittedFileName[0];

			if (!fileLogName.equals(logName))
				continue;

			PartitionAndFile partitionAndFile = parse(splittedFileName[1]);
			if (partitionAndFile != null && partitionAndFile.logPartition.equals(logPartition)) {
				entries.add(partitionAndFile.logFile);
			}
		}
		return entries;
	}
}
