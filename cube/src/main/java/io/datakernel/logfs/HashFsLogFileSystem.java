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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.hashfs.HashFsClient;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.util.List;

public final class HashFsLogFileSystem extends AbstractRemoteLogFileSystem {
	private final HashFsClient client;

	public HashFsLogFileSystem(HashFsClient client, String logName) {
		super(logName);
		this.client = client;
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
	public void read(String logPartition, LogFile logFile, long startPosition, StreamConsumer<ByteBuf> consumer, ResultCallback<Long> positionCallback) {
		client.download(path(logPartition, logFile), startPosition, consumer, positionCallback);
	}

	@Override
	public void write(String logPartition, LogFile logFile, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		client.upload(path(logPartition, logFile), producer, callback);
	}
}
