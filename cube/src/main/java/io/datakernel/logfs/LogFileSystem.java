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
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;

import java.util.List;

/**
 * Represents a file system where logs are persisted.
 */
public interface LogFileSystem {
	void makeUniqueLogFile(String logPartition, String logName, ResultCallback<LogFile> callback);

	void list(String logPartition, ResultCallback<List<LogFile>> callback);

	void read(String logPartition, LogFile logFile, long startPosition, StreamConsumer<ByteBuf> consumer,
	          ResultCallback<Long> positionCallback);

	void write(String logPartition, LogFile logFile, StreamProducer<ByteBuf> producer, CompletionCallback callback);
}