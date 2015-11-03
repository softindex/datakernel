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

/**
 * Manages persistence of logs.
 */
public interface LogManager<T> {
	/**
	 * Creates a {@code StreamConsumer} that persists streamed log items to log.
	 *
	 * @param streamId id of stream (log partition name)
	 * @return StreamConsumer, which will write records, streamed from wired producer.
	 */
	LogStreamConsumer<T> consumer(String streamId);

	/**
	 * Creates a {@code StreamProducer} that streams items, contained in a given partition and file, starting at the specified position.
	 *
	 * @param logPartition     name of log partition
	 * @param logFile          log file
	 * @param position         position
	 * @param positionCallback callback which is called once streaming is done. Final read position is passed to callback.
	 * @return StreamProducer, which will stream read items to its wired consumer.
	 */
	LogStreamProducer<T> producer(String logPartition, LogFile logFile, long position,
	                           ResultCallback<LogPosition> positionCallback);
}
