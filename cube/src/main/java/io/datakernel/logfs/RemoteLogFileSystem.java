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

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.stats.StreamRegistry;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsDetailed;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;
import static io.datakernel.stream.stats.StreamStatsSizeCounter.forByteBufs;

public final class RemoteLogFileSystem extends AbstractLogFileSystem implements EventloopJmxMBeanEx {
	private static final String LOG_NAME_DELIMITER = "/";

	private final Eventloop eventloop;
	private final String logName;
	private final RemoteFsClient client;

	private final StageStats stageList = StageStats.create(SMOOTHING_WINDOW_5_MINUTES);
	private final StageStats stageRead = StageStats.create(SMOOTHING_WINDOW_5_MINUTES);
	private final StageStats stageWrite = StageStats.create(SMOOTHING_WINDOW_5_MINUTES);

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
	public Stage<List<LogFile>> list(String logPartition) {
		return client.list().thenApply(files -> getLogFiles(files, logPartition)).whenComplete(stageList.recordStats());
	}

	@Override
	public Stage<StreamProducerWithResult<ByteBuf, Void>> read(String logPartition, LogFile logFile, long startPosition) {
		return client.download(path(logPartition, logFile), startPosition)
				.thenApply(stream -> stream
						.with(streamReads.newEntry(logPartition + ":" + logFile + "@" + startPosition))
						.with(streamReadStats)
						.withLateBinding()
				)
				.whenComplete(stageRead.recordStats());
	}

	@Override
	public Stage<StreamConsumerWithResult<ByteBuf, Void>> write(String logPartition, LogFile logFile) {
		String fileName = path(logPartition, logFile);
		return client.upload(fileName)
				.thenApply(stream -> stream
						.with(streamWrites.newEntry(logPartition + ":" + logFile))
						.with(streamWriteStats)
						.withLateBinding()
				)
				.whenComplete(stageWrite.recordStats());
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
	public StageStats getStageList() {
		return stageList;
	}

	@JmxAttribute
	public StageStats getStageRead() {
		return stageRead;
	}

	@JmxAttribute
	public StageStats getStageWrite() {
		return stageWrite;
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
