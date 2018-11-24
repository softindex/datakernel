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
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.stream.stats.StreamRegistry;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsDetailed;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.stream.stats.StreamStatsSizeCounter.forByteBufs;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("rawtypes") // JMX doesn't work with generic types
public final class RemoteLogFileSystem extends AbstractLogFileSystem implements EventloopJmxMBeanEx {
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);
	private static final String LOG_NAME_DELIMITER = "/";

	private final Eventloop eventloop;
	private final String logName;
	private final RemoteFsClient client;

	private final PromiseStats promiseList = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseRead = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseWrite = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	private final StreamRegistry<String> streamReads = StreamRegistry.create();
	private final StreamRegistry<String> streamWrites = StreamRegistry.create();

	private final StreamStatsDetailed<ByteBuf> streamReadStats = StreamStats.detailed(forByteBufs());
	private final StreamStatsDetailed<ByteBuf> streamWriteStats = StreamStats.detailed(forByteBufs());

	private RemoteLogFileSystem(Eventloop eventloop, String logName, RemoteFsClient client) {
		this.eventloop = eventloop;
		this.logName = logName;
		this.client = client;
	}

	public static RemoteLogFileSystem create(Eventloop eventloop, String logName, RemoteFsClient client) {
		return new RemoteLogFileSystem(eventloop, logName, client);
	}

	@Override
	public Promise<List<LogFile>> list(String logPartition) {
		return client.list()
				.thenApply(files -> getLogFiles(files.stream().map(FileMetadata::getFilename).collect(toList()), logPartition))
				.whenComplete(promiseList.recordStats());
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> read(String logPartition, LogFile logFile, long startPosition) {
		return client.download(path(logPartition, logFile), startPosition)
				.thenApply(stream -> stream
						.transformWith(streamReads.register(logPartition + ":" + logFile + "@" + startPosition))
						.transformWith(streamReadStats))
				.whenComplete(promiseRead.recordStats());
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> write(String logPartition, LogFile logFile) {
		String fileName = path(logPartition, logFile);
		return client.upload(fileName)
				.thenApply(stream -> stream
						.transformWith(streamWrites.register(logPartition + ":" + logFile))
						.transformWith(streamWriteStats))
				.whenComplete(promiseWrite.recordStats());
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

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public PromiseStats getPromiseList() {
		return promiseList;
	}

	@JmxAttribute
	public PromiseStats getPromiseRead() {
		return promiseRead;
	}

	@JmxAttribute
	public PromiseStats getPromiseWrite() {
		return promiseWrite;
	}

	@JmxAttribute
	public StreamRegistry getStreamReads() {
		return streamReads;
	}

	@JmxAttribute
	public StreamRegistry getStreamWrites() {
		return streamWrites;
	}

	@JmxAttribute
	public StreamStatsDetailed getStreamReadStats() {
		return streamReadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getStreamWriteStats() {
		return streamWriteStats;
	}

}
