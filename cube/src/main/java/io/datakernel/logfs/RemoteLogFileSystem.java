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

import io.datakernel.FsClient;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;

import java.util.ArrayList;
import java.util.List;

public final class RemoteLogFileSystem extends AbstractLogFileSystem {
	private static final String LOG_NAME_DELIMITER = "/";

	private final Eventloop eventloop;
	private final String logName;
	private final FsClient client;

	private RemoteLogFileSystem(Eventloop eventloop, String logName, FsClient client) {
		this.eventloop = eventloop;
		this.logName = logName;
		this.client = client;
	}

	public static RemoteLogFileSystem create(Eventloop eventloop, String logName, FsClient client) {
		return new RemoteLogFileSystem(eventloop, logName, client);
	}

	@Override
	public void list(final String logPartition, final ResultCallback<List<LogFile>> callback) {
		client.list(new ResultCallback<List<String>>() {
			@Override
			public void onResult(List<String> files) {
				callback.onResult(getLogFiles(files, logPartition));
			}

			@Override
			public void onException(Exception exception) {
				callback.onException(exception);
			}
		});
	}

	@Override
	public void read(String logPartition, LogFile logFile, long startPosition, final StreamConsumer<ByteBuf> consumer) {
		client.download(path(logPartition, logFile), startPosition, new ResultCallback<StreamProducer<ByteBuf>>() {
			@Override
			public void onResult(StreamProducer<ByteBuf> producer) {
				producer.streamTo(consumer);
			}

			@Override
			public void onException(Exception e) {
				StreamProducers.<ByteBuf>closingWithError(eventloop, e).streamTo(consumer);
			}
		});
	}

	@Override
	public void write(String logPartition, LogFile logFile, StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		final String fileName = path(logPartition, logFile);
		client.upload(fileName, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				client.delete(fileName, AsyncCallbacks.ignoreCompletionCallback());
				callback.onException(e);
			}
		});
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
