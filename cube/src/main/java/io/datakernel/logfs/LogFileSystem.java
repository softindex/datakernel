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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Represents a file system where logs are persisted.
 */
public interface LogFileSystem {
	CompletionStage<LogFile> makeUniqueLogFile(String logPartition, String logName);

	CompletionStage<List<LogFile>> list(String logPartition);

	CompletionStage<StreamProducerWithResult<ByteBuf, Void>> read(String logPartition, LogFile logFile, long startPosition);

	default StreamProducerWithResult<ByteBuf, Void> readStream(String logPartition, LogFile logFile, long startPosition) {
		return StreamProducerWithResult.ofStage(read(logPartition, logFile, startPosition));
	}

	CompletionStage<StreamConsumerWithResult<ByteBuf, Void>> write(String logPartition, LogFile logFile);

	default StreamConsumerWithResult<ByteBuf, Void> writeStream(String logPartition, LogFile logFile) {
		return StreamConsumerWithResult.ofStage(write(logPartition, logFile));
	}
}