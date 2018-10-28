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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;

import java.util.List;

/**
 * Represents a file system where logs are persisted.
 */
public interface LogFileSystem {
	Promise<LogFile> makeUniqueLogFile(String logPartition, String logName);

	Promise<List<LogFile>> list(String logPartition);

	Promise<SerialSupplier<ByteBuf>> read(String logPartition, LogFile logFile, long startPosition);

	default SerialSupplier<ByteBuf> readStream(String logPartition, LogFile logFile, long startPosition) {
		return SerialSupplier.ofPromise(read(logPartition, logFile, startPosition));
	}

	Promise<SerialConsumer<ByteBuf>> write(String logPartition, LogFile logFile);

	default SerialConsumer<ByteBuf> writeStream(String logPartition, LogFile logFile) {
		return SerialConsumer.ofPromise(write(logPartition, logFile));
	}
}
