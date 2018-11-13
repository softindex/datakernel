/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
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
	public MaterializedPromise<Void> set(SerialSupplier<ByteBuf> input) {
		this.input = input;
		if (this.input != null) startProcess();
		return getProcessResult();
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenResult(buf -> {
					if (buf != null)
						ensureConsumer()
								.whenResult($1 ->
										currentConsumer.accept(buf)
												.whenResult($2 -> {
													if (isProcessComplete()) return;
													doProcess();
												})
												.whenException(this::close))
								.whenException(e -> buf.recycle())
								.whenException(this::close);
					else {
						flush()
								.whenResult($ -> completeProcess())
								.whenException(this::close);
					}
				})
				.whenException(this::close);
	}

	private Promise<Void> ensureConsumer() {
		String chunkName = datetimeFormat.format(Instant.ofEpochMilli(currentTimeProvider.currentTimeMillis()));
		return chunkName.equals(currentChunkName) ?
				Promise.complete() :
				startNewChunk(chunkName);
	}

	private Promise<Void> startNewChunk(String chunkName) {
		return flush()
				.thenCompose($ -> {
					if (isProcessComplete()) return Promise.complete();
					currentChunkName = chunkName;
					return fileSystem.makeUniqueLogFile(logPartition, chunkName)
							.thenCompose(newLogFile -> fileSystem.write(logPartition, newLogFile)
									.whenResult(newConsumer -> {
										if (isProcessComplete()) {
											newConsumer.cancel();
											return;
										}
										currentConsumer = newConsumer;
									}))
							.toVoid();
				});
	}

	private Promise<Void> flush() {
		if (currentConsumer == null) return Promise.complete();
		//noinspection AssignmentToNull - consumer is ensured to be not null, later on
		return currentConsumer.accept(null)
				.whenResult($ -> currentConsumer = null);
	}

	@Override
	protected void doClose(Throwable e) {
		input.close(e);
		if (currentConsumer != null) {
			currentConsumer.close(e);
		}
	}
}
