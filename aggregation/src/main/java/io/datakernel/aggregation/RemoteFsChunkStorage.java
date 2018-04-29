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

package io.datakernel.aggregation;

import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.StageStats;
import io.datakernel.remotefs.IRemoteFsClient;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerModifier;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.processor.*;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsBasic;
import io.datakernel.stream.stats.StreamStatsDetailed;
import io.datakernel.util.MemSize;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.datakernel.aggregation.AggregationUtils.createBufferSerializer;
import static io.datakernel.stream.stats.StreamStatsSizeCounter.forByteBufs;
import static java.util.stream.Collectors.toMap;

public final class RemoteFsChunkStorage implements AggregationChunkStorage, EventloopJmxMBeanEx {
	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(256);
	public static final String LOG = ".log";
	public static final String TEMP_LOG = ".temp";
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final IRemoteFsClient client;
	private final IdGenerator<Long> idGenerator;

	private MemSize bufferSize = DEFAULT_BUFFER_SIZE;

	private final StageStats stageIdGenerator = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenR1 = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenR2 = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenR3 = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenW = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageFinishChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	private boolean detailed;

	private final StreamStatsDetailed<ByteBuf> readRemoteFS = StreamStats.detailed(forByteBufs());
	private final StreamStatsDetailed<ByteBuf> readDecompress = StreamStats.detailed(forByteBufs());
	private final StreamStatsBasic<?> readDeserialize = StreamStats.basic();
	private final StreamStatsDetailed<?> readDeserializeDetailed = StreamStats.detailed();

	private final StreamStatsBasic<?> writeSerialize = StreamStats.basic();
	private final StreamStatsDetailed<?> writeSerializeDetailed = StreamStats.detailed();
	private final StreamStatsDetailed<ByteBuf> writeCompress = StreamStats.detailed(forByteBufs());
	private final StreamStatsDetailed<ByteBuf> writeChunker = StreamStats.detailed(forByteBufs());
	private final StreamStatsDetailed<ByteBuf> writeRemoteFS = StreamStats.detailed(forByteBufs());

	private RemoteFsChunkStorage(Eventloop eventloop, IdGenerator<Long> idGenerator, InetSocketAddress serverAddress) {
		this.eventloop = eventloop;
		this.idGenerator = idGenerator;
		this.client = RemoteFsClient.create(eventloop, serverAddress);
	}

	public static RemoteFsChunkStorage create(Eventloop eventloop, IdGenerator<Long> idGenerator, InetSocketAddress serverAddress) {
		return new RemoteFsChunkStorage(eventloop, idGenerator, serverAddress);
	}

	public RemoteFsChunkStorage withBufferSize(MemSize bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Stage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields,
	                                                         Class<T> recordClass, long chunkId,
	                                                         DefiningClassLoader classLoader) {
		return client.download(path(chunkId), 0)
				.whenComplete(stageOpenR1.recordStats())
				.thenApply(producer -> producer
						.with(readRemoteFS)
						.with(StreamLZ4Decompressor.create())
						.with(readDecompress)
						.with(StreamBinaryDeserializer.create(
								createBufferSerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader)))
						.with((StreamProducerModifier<T, T>) (detailed ? readDeserializeDetailed : readDeserialize))
						.withEndOfStreamAsResult()
						.withLateBinding());
	}

	private String path(long chunkId) {
		return chunkId + LOG;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Stage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields,
	                                                          Class<T> recordClass, long chunkId,
	                                                          DefiningClassLoader classLoader) {
		return client.upload(tempPath(chunkId))
				.whenComplete(stageOpenW.recordStats())
				.thenApply(consumer -> StreamTransformer.<T>idenity()
						.with((StreamProducerModifier<T, T>) (detailed ? writeSerializeDetailed : writeSerialize))
						.with(StreamBinarySerializer.create(
								createBufferSerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader))
								.withInitialBufferSize(bufferSize))
						.with(writeCompress)
						.with(StreamLZ4Compressor.fastCompressor())
						.with(writeChunker)
						.with(StreamByteChunker.create(
								bufferSize.map(bytes -> bytes / 2),
								bufferSize.map(bytes -> bytes * 2)))
						.with(writeRemoteFS)
						.applyTo(consumer));
	}

	private String tempPath(long chunkId) {
		return chunkId + TEMP_LOG;
	}

	@Override
	public Stage<Void> finish(Set<Long> chunkIds) {
		return client.move(chunkIds.stream().collect(toMap(this::tempPath, this::path)))
				.whenComplete(stageFinishChunks.recordStats());
	}

	@Override
	public Stage<Long> createId() {
		return idGenerator.createId().whenComplete(stageIdGenerator.recordStats());
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public StageStats getStageIdGenerator() {
		return stageIdGenerator;
	}

	@JmxAttribute
	public StageStats getStageOpenR1() {
		return stageOpenR1;
	}

	@JmxAttribute
	public StageStats getStageOpenR2() {
		return stageOpenR2;
	}

	@JmxAttribute
	public StageStats getStageOpenR3() {
		return stageOpenR3;
	}

	@JmxAttribute
	public StageStats getStageOpenW() {
		return stageOpenW;
	}

	@JmxAttribute
	public StageStats getStageFinishChunks() {
		return stageFinishChunks;
	}

	@JmxAttribute
	public StreamStatsDetailed getReadRemoteFS() {
		return readRemoteFS;
	}

	@JmxAttribute
	public StreamStatsDetailed getReadDecompress() {
		return readDecompress;
	}

	@JmxAttribute
	public StreamStatsBasic getReadDeserialize() {
		return readDeserialize;
	}

	@JmxAttribute
	public StreamStatsDetailed getReadDeserializeDetailed() {
		return readDeserializeDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getWriteSerialize() {
		return writeSerialize;
	}

	@JmxAttribute
	public StreamStatsDetailed getWriteSerializeDetailed() {
		return writeSerializeDetailed;
	}

	@JmxAttribute
	public StreamStatsDetailed getWriteCompress() {
		return writeCompress;
	}

	@JmxAttribute
	public StreamStatsDetailed getWriteChunker() {
		return writeChunker;
	}

	@JmxAttribute
	public StreamStatsDetailed getWriteRemoteFS() {
		return writeRemoteFS;
	}

	@JmxOperation
	public void startDetailedMonitoring() {
		detailed = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailed = false;
	}

}