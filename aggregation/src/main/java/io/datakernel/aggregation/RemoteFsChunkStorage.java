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
import io.datakernel.async.Stages;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.StageStats;
import io.datakernel.remotefs.IRemoteFsClient;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.processor.*;
import io.datakernel.util.MemSize;
import io.datakernel.util.MutableBuilder;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static io.datakernel.aggregation.AggregationUtils.createBufferSerializer;
import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;
import static io.datakernel.stream.DataStreams.stream;
import static io.datakernel.stream.processor.StreamStatsSizeCounter.forByteBufs;
import static java.util.stream.Collectors.toMap;

public class RemoteFsChunkStorage implements AggregationChunkStorage, MutableBuilder<RemoteFsChunkStorage>, EventloopJmxMBean {
	public static final int DEFAULT_BUFFER_SIZE = 256 * 1024;
	public static final String LOG = ".log";
	public static final String TEMP_LOG = ".temp";
	public static final double DEFAULT_SMOOTHING_WINDOW = SMOOTHING_WINDOW_5_MINUTES;

	private final Eventloop eventloop;
	private final IRemoteFsClient client;
	private final IdGenerator<Long> idGenerator;

	private int bufferSize = DEFAULT_BUFFER_SIZE;

	private final StageStats stageIdGenerator = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenR1 = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenR2 = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenR3 = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenW = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageFinishChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	private final StreamStatsDetailedEx readRemoteFS = StreamStatsDetailedEx.create(forByteBufs());
	private final StreamStatsDetailedEx readDecompress = StreamStatsDetailedEx.create(forByteBufs());
	private final StreamStatsBasic readDeserialize = new StreamStatsBasic();

	private final StreamStatsBasic writeSerialize = new StreamStatsBasic();
	private final StreamStatsDetailedEx writeCompress = StreamStatsDetailedEx.create(forByteBufs());
	private final StreamStatsDetailedEx writeChunker = StreamStatsDetailedEx.create(forByteBufs());
	private final StreamStatsDetailedEx writeRemoteFS = StreamStatsDetailedEx.create(forByteBufs());

	private RemoteFsChunkStorage(Eventloop eventloop, IdGenerator<Long> idGenerator, InetSocketAddress serverAddress) {
		this.eventloop = eventloop;
		this.idGenerator = idGenerator;
		this.client = RemoteFsClient.create(eventloop, serverAddress);
	}

	public static RemoteFsChunkStorage create(Eventloop eventloop, IdGenerator<Long> idGenerator, InetSocketAddress serverAddress) {
		return new RemoteFsChunkStorage(eventloop, idGenerator, serverAddress);
	}

	public RemoteFsChunkStorage withBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public RemoteFsChunkStorage withBufferSize(MemSize bufferSize) {
		this.bufferSize = (int) bufferSize.get();
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletionStage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields,
	                                                                   Class<T> recordClass, long chunkId,
	                                                                   DefiningClassLoader classLoader) {
		return Stages.firstComplete(
				() -> stageOpenR1.monitor(client.download(path(chunkId), 0)),
				() -> stageOpenR2.monitor(client.download(tempPath(chunkId), 0)),
				() -> stageOpenR3.monitor(client.download(path(chunkId), 0)))
				.thenApply(producer -> {
					StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create();
					StreamBinaryDeserializer<T> deserializer = StreamBinaryDeserializer.create(
							createBufferSerializer(aggregation, recordClass,
									aggregation.getKeys(), fields, classLoader));

					stream(producer.with(readRemoteFS::wrap), decompressor.getInput());
					stream(decompressor.getOutput().with(readDecompress::wrap), deserializer.getInput());

					return deserializer.getOutput().with(readDeserialize::wrap).withEndOfStreamAsResult();
				});
	}

	private String path(long chunkId) {
		return chunkId + LOG;
	}

	@Override
	public <T> CompletionStage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields,
	                                                                    Class<T> recordClass, long chunkId,
	                                                                    DefiningClassLoader classLoader) {
		return stageOpenW.monitor(client.upload(tempPath(chunkId)))
				.thenApply(consumer -> {
					StreamBinarySerializer<T> serializer = StreamBinarySerializer.create(
							createBufferSerializer(aggregation, recordClass,
									aggregation.getKeys(), fields, classLoader))
							.withDefaultBufferSize(bufferSize);
					StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor();
					StreamByteChunker chunker = StreamByteChunker.create(bufferSize / 2, bufferSize * 2);

					stream(serializer.getOutput(), compressor.getInput().with(writeCompress::wrap));
					stream(compressor.getOutput(), chunker.getInput().with(writeChunker::wrap));
					stream(chunker.getOutput(), consumer.with(writeRemoteFS::wrap));

					return serializer.getInput().with(writeSerialize::wrap).withResult(consumer.getResult());
				});
	}

	private String tempPath(long chunkId) {
		return chunkId + TEMP_LOG;
	}

	@Override
	public CompletionStage<Void> finish(Set<Long> chunkIds) {
		return stageFinishChunks.monitor(
				client.move(chunkIds.stream().collect(toMap(this::tempPath, this::path))));
	}

	@Override
	public CompletionStage<Long> createId() {
		return stageIdGenerator.monitor(idGenerator.createId());
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
	public StreamStatsDetailedEx getReadRemoteFS() {
		return readRemoteFS;
	}

	@JmxAttribute
	public StreamStatsDetailedEx getReadDecompress() {
		return readDecompress;
	}

	@JmxAttribute
	public StreamStatsBasic getReadDeserialize() {
		return readDeserialize;
	}

	@JmxAttribute
	public StreamStatsBasic getWriteSerialize() {
		return writeSerialize;
	}

	@JmxAttribute
	public StreamStatsDetailedEx getWriteCompress() {
		return writeCompress;
	}

	@JmxAttribute
	public StreamStatsDetailedEx getWriteChunker() {
		return writeChunker;
	}

	@JmxAttribute
	public StreamStatsDetailedEx getWriteRemoteFS() {
		return writeRemoteFS;
	}

	@JmxOperation
	public void resetStats() {
		stageIdGenerator.resetStats();
		stageOpenR1.resetStats();
		stageOpenR2.resetStats();
		stageOpenR3.resetStats();
		stageOpenW.resetStats();
		stageFinishChunks.resetStats();

		readRemoteFS.resetStats();
		readDecompress.resetStats();
		readDeserialize.resetStats();

		writeSerialize.resetStats();
		writeCompress.resetStats();
		writeChunker.resetStats();
		writeRemoteFS.resetStats();
	}

}