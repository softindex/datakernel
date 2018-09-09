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

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialInput;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.time.CurrentTimeProvider;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

public final class LogStreamChunker extends AbstractAsyncProcess implements SerialInput<ByteBuf> {
	private final CurrentTimeProvider currentTimeProvider;
	private final DateTimeFormatter datetimeFormat;
	private final LogFileSystem fileSystem;
	private final String logPartition;

	private SerialSupplier<ByteBuf> input;
	private SerialConsumer<ByteBuf> currentConsumer;

	private String currentChunkName;

	private LogStreamChunker(CurrentTimeProvider currentTimeProvider, LogFileSystem fileSystem, DateTimeFormatter datetimeFormat, String logPartition) {
		this.currentTimeProvider = currentTimeProvider;
		this.datetimeFormat = datetimeFormat;
		this.fileSystem = fileSystem;
		this.logPartition = logPartition;
	}

	public static LogStreamChunker create(LogFileSystem fileSystem, DateTimeFormatter datetimeFormat, String logPartition) {
		return create(Eventloop.getCurrentEventloop(), fileSystem, datetimeFormat, logPartition);
	}

	static LogStreamChunker create(CurrentTimeProvider currentTimeProvider, LogFileSystem fileSystem, DateTimeFormatter datetimeFormat, String logPartition) {
		return new LogStreamChunker(currentTimeProvider, fileSystem, datetimeFormat, logPartition);
	}

	@Override
	public void setInput(SerialSupplier<ByteBuf> input) {
		this.input = input;
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenResult(buf -> {
					if (isProcessComplete()) {
						if (buf != null) buf.recycle();
						return;
					}
					if (buf != null) {
						ensureConsumer()
								.thenRun(() -> {
									if (isProcessComplete()) {
										buf.recycle();
										return;
									}

									currentConsumer.accept(buf)
											.thenRun(() -> {
												if (isProcessComplete()) return;
												doProcess();
											})
											.whenException(this::closeWithError);
								})
								.whenException(e -> buf.recycle())
								.whenException(this::closeWithError);
					} else {
						flush()
								.thenRun(this::completeProcess)
								.whenException(this::closeWithError);
					}
				})
				.whenException(this::closeWithError);
	}

	private Stage<Void> ensureConsumer() {
		String chunkName = datetimeFormat.format(Instant.ofEpochMilli(currentTimeProvider.currentTimeMillis()));
		return chunkName.equals(currentChunkName) ?
				Stage.complete() :
				startNewChunk(chunkName);
	}

	private Stage<Void> startNewChunk(String chunkName) {
		return flush()
				.thenCompose($ -> {
					if (isProcessComplete()) return Stage.complete();
					currentChunkName = chunkName;
					return fileSystem.makeUniqueLogFile(logPartition, chunkName)
							.thenCompose(newLogFile -> fileSystem.write(logPartition, newLogFile)
									.whenResult(newConsumer -> {
										if (isProcessComplete()) {
											newConsumer.cancel();
											return;
										}
										this.currentConsumer = newConsumer;
									}))
							.toVoid();
				});
	}

	private Stage<Void> flush() {
		if (currentConsumer == null) return Stage.complete();
		return currentConsumer.accept(null)
				.thenRun(() -> currentConsumer = null);
	}

	@Override
	protected void doCloseWithError(Throwable e) {
		input.closeWithError(e);
		if (currentConsumer != null) {
			currentConsumer.closeWithError(e);
		}
	}
}
