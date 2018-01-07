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

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import io.datakernel.time.CurrentTimeProvider;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletionStage;

import static io.datakernel.async.SettableStage.mirrorOf;

public final class LogStreamChunker extends StreamConsumerDecorator<ByteBuf> implements StreamConsumerWithResult<ByteBuf, Void>, StreamDataReceiver<ByteBuf> {
	private final CurrentTimeProvider currentTimeProvider;
	private final DateTimeFormatter datetimeFormat;
	private final LogFileSystem fileSystem;
	private final String logPartition;

	private final StreamConsumerSwitcher<ByteBuf> switcher;
	private final SettableStage<Void> result;

	private String currentChunkName;
	private StreamDataReceiver<ByteBuf> dataReceiver;
	private StreamConsumerWithResult<ByteBuf, Void> currentConsumer;

	private LogStreamChunker(CurrentTimeProvider currentTimeProvider, LogFileSystem fileSystem, DateTimeFormatter datetimeFormat, String logPartition,
	                         StreamConsumerSwitcher<ByteBuf> switcher) {
		super(switcher);
		this.currentTimeProvider = currentTimeProvider;
		this.datetimeFormat = datetimeFormat;
		this.fileSystem = fileSystem;
		this.logPartition = logPartition;
		this.switcher = switcher;
		this.result = mirrorOf(getEndOfStream().thenCompose($ -> currentConsumer.getResult()));
	}

	public static LogStreamChunker create(LogFileSystem fileSystem, DateTimeFormatter datetimeFormat,
	                                      String logPartition) {
		return create(Eventloop.getCurrentEventloop(), fileSystem, datetimeFormat, logPartition);
	}

	static LogStreamChunker create(CurrentTimeProvider currentTimeProvider, LogFileSystem fileSystem, DateTimeFormatter datetimeFormat,
	                               String logPartition) {
		StreamConsumerSwitcher<ByteBuf> switcher = StreamConsumerSwitcher.create();
		LogStreamChunker chunker = new LogStreamChunker(currentTimeProvider, fileSystem, datetimeFormat, logPartition, switcher);
		long timestamp = currentTimeProvider.currentTimeMillis();
		String chunkName = datetimeFormat.format(Instant.ofEpochMilli(timestamp));
		chunker.startNewChunk(chunkName, Stages.of(null));
		return chunker;
	}

	@Override
	public void onData(ByteBuf item) {
		String chunkName = datetimeFormat.format(Instant.ofEpochMilli(currentTimeProvider.currentTimeMillis()));
		if (!chunkName.equals(currentChunkName)) {
			startNewChunk(chunkName, currentConsumer.getResult());
		}

		dataReceiver.onData(item);
	}

	@Override
	protected StreamDataReceiver<ByteBuf> onProduce(StreamDataReceiver<ByteBuf> dataReceiver) {
		this.dataReceiver = dataReceiver;
		return this;
	}

	private void startNewChunk(String newChunkName, CompletionStage<Void> previousFile) {
		currentChunkName = newChunkName;
		currentConsumer = StreamConsumers.ofStageWithResult(previousFile
				.thenCompose($ -> fileSystem.makeUniqueLogFile(logPartition, newChunkName))
				.thenCompose(logFile -> fileSystem.write(logPartition, logFile)));

		switcher.switchTo(currentConsumer);
	}

	@Override
	public CompletionStage<Void> getResult() {
		return result;
	}
}