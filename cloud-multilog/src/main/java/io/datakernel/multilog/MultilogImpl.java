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

package io.datakernel.multilog;

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.process.ChannelDeserializer;
import io.datakernel.csp.process.ChannelLZ4Compressor;
import io.datakernel.csp.process.ChannelLZ4Decompressor;
import io.datakernel.csp.process.ChannelSerializer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.TruncatedDataException;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.StreamSupplierWithResult;
import io.datakernel.stream.stats.StreamRegistry;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsDetailed;
import io.datakernel.util.MemSize;
import io.datakernel.util.Preconditions;
import io.datakernel.util.Stopwatch;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static io.datakernel.stream.stats.StreamStatsSizeCounter.forByteBufs;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public final class MultilogImpl<T> implements Multilog<T>, EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(MultilogImpl.class);

	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(256);

	private final Eventloop eventloop;
	private final FsClient client;
	private final LogNamingScheme namingScheme;
	private final BinarySerializer<T> serializer;

	private MemSize bufferSize = DEFAULT_BUFFER_SIZE;
	private Duration autoFlushInterval = null;

	private final StreamRegistry<String> streamReads = StreamRegistry.create();
	private final StreamRegistry<String> streamWrites = StreamRegistry.create();

	private final StreamStatsDetailed<ByteBuf> streamReadStats = StreamStats.detailed(forByteBufs());
	private final StreamStatsDetailed<ByteBuf> streamWriteStats = StreamStats.detailed(forByteBufs());

	private MultilogImpl(Eventloop eventloop, FsClient client, BinarySerializer<T> serializer,
			LogNamingScheme namingScheme) {
		this.eventloop = eventloop;
		this.client = client;
		this.serializer = serializer;
		this.namingScheme = namingScheme;
	}

	public static <T> MultilogImpl<T> create(Eventloop eventloop, FsClient client,
			BinarySerializer<T> serializer, LogNamingScheme namingScheme) {
		return new MultilogImpl<>(eventloop, client, serializer, namingScheme);
	}

	public MultilogImpl<T> withBufferSize(int bufferSize) {
		this.bufferSize = MemSize.of(bufferSize);
		return this;
	}

	public MultilogImpl<T> withBufferSize(MemSize bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public MultilogImpl<T> withAutoFlushInterval(Duration autoFlushInterval) {
		this.autoFlushInterval = autoFlushInterval;
		return this;
	}

	@Override
	public Promise<StreamConsumer<T>> write(@NotNull String logPartition) {
		validateLogPartition(logPartition);

		return Promise.of(StreamConsumer.<T>ofSupplier(
				supplier -> supplier
						.transformWith(ChannelSerializer.create(serializer)
								.withAutoFlushInterval(autoFlushInterval)
								.withInitialBufferSize(bufferSize)
								.withSkipSerializationErrors())
						.transformWith(ChannelLZ4Compressor.createFastCompressor())
						.transformWith(streamWrites.register(logPartition))
						.transformWith(streamWriteStats)
						.bindTo(new LogStreamChunker(eventloop, client, namingScheme, logPartition)))
				.withLateBinding());
	}

	@Override
	public Promise<StreamSupplierWithResult<T, LogPosition>> read(@NotNull String logPartition,
			@NotNull LogFile startLogFile, long startOffset,
			@Nullable LogFile endLogFile) {
		validateLogPartition(logPartition);
		LogPosition startPosition = LogPosition.create(startLogFile, startOffset);
		return client.list("**")
				.thenApply(files ->
						files.stream()
								.map(FileMetadata::getName)
								.map(namingScheme::parse)
								.filter(Objects::nonNull)
								.filter(partitionAndFile -> partitionAndFile.getLogPartition().equals(logPartition))
								.map(PartitionAndFile::getLogFile)
								.collect(toList()))
				.thenApply(logFiles ->
						logFiles.stream()
								.filter(logFile -> isFileInRange(logFile, startPosition, endLogFile))
								.sorted()
								.collect(toList()))
				.thenApply(logFilesToRead -> readLogFiles(logPartition, startPosition, logFilesToRead));
	}

	private StreamSupplierWithResult<T, LogPosition> readLogFiles(@NotNull String logPartition, @NotNull LogPosition startPosition, @NotNull List<LogFile> logFiles) {
		SettablePromise<LogPosition> positionPromise = new SettablePromise<>();

		Iterator<StreamSupplier<T>> logFileStreams = new Iterator<StreamSupplier<T>>() {
			final Stopwatch sw = Stopwatch.createUnstarted();

			final Iterator<LogFile> it = logFiles.iterator();
			int n;
			LogFile currentLogFile;
			long inputStreamPosition;

			@Override
			public boolean hasNext() {
				if (it.hasNext()) return true;
				positionPromise.set(getLogPosition());
				return false;
			}

			LogPosition getLogPosition() {
				if (currentLogFile == null)
					return startPosition;

				if (currentLogFile.equals(startPosition.getLogFile()))
					return LogPosition.create(currentLogFile, startPosition.getPosition() + inputStreamPosition);

				return LogPosition.create(currentLogFile, inputStreamPosition);
			}

			@Override
			public StreamSupplier<T> next() {
				currentLogFile = it.next();
				long position = n++ == 0 ? startPosition.getPosition() : 0L;

				if (logger.isTraceEnabled())
					logger.trace("Read log file `{}` from: {}", currentLogFile, position);

				return StreamSupplier.ofPromise(
						client.download(namingScheme.path(logPartition, currentLogFile), position)
								.thenApply(fileStream -> {
									inputStreamPosition = 0L;
									sw.reset().start();
									return fileStream
											.transformWith(streamReads.register(logPartition + ":" + currentLogFile + "@" + position))
											.transformWith(streamReadStats)
											.transformWith(ChannelLZ4Decompressor.create()
													.withInspector(new ChannelLZ4Decompressor.Inspector() {
														@Override
														public <Q extends ChannelLZ4Decompressor.Inspector> Q lookup(Class<Q> type) {
															throw new UnsupportedOperationException();
														}

														@Override
														public void onBlock(ChannelLZ4Decompressor self, ChannelLZ4Decompressor.Header header, ByteBuf inputBuf, ByteBuf outputBuf) {
															inputStreamPosition += ChannelLZ4Decompressor.HEADER_LENGTH + header.compressedLen;
														}
													}))
											.transformWith(supplier ->
													supplier.withEndOfStream(eos ->
															eos.thenComposeEx(($, e) -> (e == null || e instanceof TruncatedDataException) ?
																	Promise.complete() :
																	Promise.ofException(e))))
											.transformWith(ChannelDeserializer.create(serializer))
											.withEndOfStream(eos ->
													eos.whenComplete(($, e) -> log(e)))
											.withLateBinding();
								}));
			}

			private void log(Throwable e) {
				if (e == null && logger.isTraceEnabled()) {
					logger.trace("Finish log file `{}` in {}, compressed bytes: {} ({} bytes/s)", currentLogFile,
							sw, inputStreamPosition, inputStreamPosition / Math.max(sw.elapsed(SECONDS), 1));
				} else if (e != null && logger.isErrorEnabled()) {
					logger.error("Error on log file `{}` in {}, compressed bytes: {} ({} bytes/s)", currentLogFile,
							sw, inputStreamPosition, inputStreamPosition / Math.max(sw.elapsed(SECONDS), 1), e);
				}
			}
		};

		return StreamSupplierWithResult.of(
				StreamSupplier.concat(logFileStreams).withLateBinding(),
				positionPromise);
	}

	private static void validateLogPartition(@NotNull String logPartition) {
		Preconditions.checkArgument(!logPartition.contains("-"), "Using dash (-) in log partition name is not allowed");
	}

	private boolean isFileInRange(@NotNull LogFile logFile, @NotNull LogPosition startPosition, @Nullable LogFile endFile) {
		if (logFile.compareTo(startPosition.getLogFile()) < 0)
			return false;

		return endFile == null || logFile.compareTo(endFile) <= 0;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

}
