/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.datakernel.crdt.local;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.crdt.CrdtData;
import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.crdt.CrdtStorage;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelDeserializer;
import io.datakernel.csp.process.ChannelSerializer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataAcceptor;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.StreamMapper;
import io.datakernel.stream.processor.StreamReducerSimple;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsBasic;
import io.datakernel.stream.stats.StreamStatsDetailed;
import io.datakernel.util.Initializable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public final class CrdtStorageFileSystem<K extends Comparable<K>, S> implements CrdtStorage<K, S>,
		Initializable<CrdtStorageFileSystem<K, S>>, EventloopService, EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CrdtStorageFileSystem.class);

	private final Eventloop eventloop;
	private final FsClient client;
	private final BinaryOperator<CrdtData<K, S>> combiner;
	private final CrdtDataSerializer<K, S> serializer;

	private Function<String, String> namingStrategy = ext -> UUID.randomUUID().toString() + "." + ext;
	private Duration consolidationMargin = Duration.ofMinutes(30);

	private FsClient consolidationFolderClient;
	private FsClient tombstoneFolderClient;

	// region JMX
	private boolean detailedStats;

	private final StreamStatsBasic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();

	private final PromiseStats consolidationStats = PromiseStats.create(Duration.ofMinutes(5));
	// endregion

	// region creators
	private CrdtStorageFileSystem(
			Eventloop eventloop,
			FsClient client,
			BinaryOperator<CrdtData<K, S>> combiner,
			CrdtDataSerializer<K, S> serializer,
			FsClient consolidationFolderClient,
			FsClient tombstoneFolderClient
	) {
		this.eventloop = eventloop;
		this.client = client;
		this.combiner = combiner;
		this.serializer = serializer;
		this.consolidationFolderClient = consolidationFolderClient;
		this.tombstoneFolderClient = tombstoneFolderClient;
	}

	public static <K extends Comparable<K>, S> CrdtStorageFileSystem<K, S> create(
			Eventloop eventloop, FsClient client, BinaryOperator<S> combiner,
			CrdtDataSerializer<K, S> serializer) {
		return new CrdtStorageFileSystem<>(eventloop, client, (a, b) -> new CrdtData<>(a.getKey(), combiner.apply(a.getState(), b.getState())),
				serializer, client.subfolder(".consolidation"), client.subfolder(".tombstones"));
	}

	public static <K extends Comparable<K>, S> CrdtStorageFileSystem<K, S> create(
			Eventloop eventloop, FsClient client, BinaryOperator<S> combiner,
			BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		return create(eventloop, client, combiner, new CrdtDataSerializer<>(keySerializer, stateSerializer));
	}

	public CrdtStorageFileSystem<K, S> withConsolidationMargin(Duration consolidationMargin) {
		this.consolidationMargin = consolidationMargin;
		return this;
	}

	public CrdtStorageFileSystem<K, S> withNamingStrategy(Function<String, String> namingStrategy) {
		this.namingStrategy = namingStrategy;
		return this;
	}

	public CrdtStorageFileSystem<K, S> withConsolidationFolder(String subfolder) {
		consolidationFolderClient = client.subfolder(subfolder);
		return this;
	}

	public CrdtStorageFileSystem<K, S> withTombstoneFolder(String subfolder) {
		tombstoneFolderClient = client.subfolder(subfolder);
		return this;
	}

	public CrdtStorageFileSystem<K, S> withConsolidationFolderClient(FsClient consolidationFolderClient) {
		this.consolidationFolderClient = consolidationFolderClient;
		return this;
	}

	public CrdtStorageFileSystem<K, S> withTombstoneFolderClient(FsClient tombstoneFolderClient) {
		this.tombstoneFolderClient = tombstoneFolderClient;
		return this;
	}
	// endregion

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return client.upload(namingStrategy.apply("bin"))
				.map(consumer -> StreamConsumer.<CrdtData<K, S>>ofSupplier(supplier -> supplier
						.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
						.transformWith(ChannelSerializer.create(serializer))
						.streamTo(consumer))
						.withLateBinding());
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return Promises.toTuple(FileLists::new, client.list("*"), tombstoneFolderClient.list("*"))
				.map(f -> {
					StreamReducerSimple<K, CrdtReducingData<K, S>, CrdtData<K, S>, CrdtAccumulator<K, S>> reducer =
							StreamReducerSimple.create(crd -> crd.key, Comparator.naturalOrder(), new CrdtReducer<>(combiner));

					Stream<FileMetadata> stream = f.files.stream();
					(timestamp == 0 ? stream : stream.filter(m -> m.getTimestamp() >= timestamp))
							.forEach(meta -> ChannelSupplier.ofPromise(client.download(meta.getName()))
									.transformWith(ChannelDeserializer.create(serializer))
									.transformWith(StreamMapper.create(data -> new CrdtReducingData<>(data.getKey(), data.getState(), meta.getTimestamp())))
									.streamTo(reducer.newInput()));

					stream = f.tombstones.stream();
					(timestamp == 0 ? stream : stream.filter(m -> m.getTimestamp() >= timestamp))
							.forEach(meta -> ChannelSupplier.ofPromise(tombstoneFolderClient.download(meta.getName()))
									.transformWith(ChannelDeserializer.create(serializer.getKeySerializer()))
									.transformWith(StreamMapper.create(key -> new CrdtReducingData<>(key, (S) null, meta.getTimestamp())))
									.streamTo(reducer.newInput()));

					return reducer.getOutput()
							.transformWith(detailedStats ? downloadStatsDetailed : downloadStats)
							.withLateBinding();
				});
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return tombstoneFolderClient.upload(namingStrategy.apply("tomb"))
				.map(consumer -> StreamConsumer.<K>ofSupplier(supplier -> supplier
						.transformWith(detailedStats ? removeStatsDetailed : removeStats)
						.transformWith(ChannelSerializer.create(serializer.getKeySerializer()))
						.streamTo(consumer))
						.withLateBinding());
	}

	@Override
	public Promise<Void> ping() {
		return client.ping();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		return Promise.complete();
	}

	public Promise<Void> consolidate() {
		long barrier = eventloop.currentInstant().minus(consolidationMargin).toEpochMilli();
		Set<String> blacklist = new HashSet<>();

		return consolidationFolderClient.list("*")
				.then(list ->
						Promises.all(list.stream()
								.filter(meta -> meta.getTimestamp() > barrier)
								.map(meta -> ChannelSupplier.ofPromise(client.download(meta.getName()))
										.toCollector(ByteBufQueue.collector())
										.whenResult(byteBuf -> blacklist.addAll(Arrays.asList(byteBuf.asString(UTF_8).split("\n"))))
										.toVoid())))
				.then($ -> client.list("*"))
				.then(list -> {
					String name = namingStrategy.apply("bin");
					List<String> files = list.stream()
							.map(FileMetadata::getName)
							.filter(fileName -> !blacklist.contains(fileName))
							.collect(toList());
					String dump = String.join("\n", files);

					logger.info("started consolidating into {} from {}", name, files);

					String metafile = namingStrategy.apply("dump");
					return consolidationFolderClient.upload(metafile)
							.then(consumer ->
									ChannelSupplier.of(ByteBuf.wrapForReading(dump.getBytes(UTF_8)))
											.streamTo(consumer))
							.then($ -> download())
							.then(producer -> producer
									.transformWith(ChannelSerializer.create(serializer))
									.streamTo(ChannelConsumer.ofPromise(client.upload(name))))
							.then($ -> tombstoneFolderClient.delete("*"))
							.then($ -> consolidationFolderClient.delete(metafile))
							.then($ -> Promises.all(files.stream().map(client::delete)));
				})
				.whenComplete(consolidationStats.recordStats());
	}

	static class CrdtReducingData<K extends Comparable<K>, S> {
		final K key;
		@Nullable
		final S state;
		final long timestamp;

		CrdtReducingData(K key, @Nullable S state, long timestamp) {
			this.key = key;
			this.state = state;
			this.timestamp = timestamp;
		}
	}

	static class CrdtAccumulator<K extends Comparable<K>, S> {
		@Nullable
		CrdtData<K, S> accumulated;
		long maxAppendTimestamp;
		long maxRemoveTimestamp;

		CrdtAccumulator(@Nullable CrdtData<K, S> accumulated, long maxAppendTimestamp, long maxRemoveTimestamp) {
			this.accumulated = accumulated;
			this.maxAppendTimestamp = maxAppendTimestamp;
			this.maxRemoveTimestamp = maxRemoveTimestamp;
		}
	}

	static class CrdtReducer<K extends Comparable<K>, S> implements StreamReducers.Reducer<K, CrdtReducingData<K, S>, CrdtData<K, S>, CrdtAccumulator<K, S>> {

		private final BinaryOperator<CrdtData<K, S>> combiner;

		public CrdtReducer(BinaryOperator<CrdtData<K, S>> combiner) {
			this.combiner = combiner;
		}

		@Override
		public CrdtAccumulator<K, S> onFirstItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtReducingData<K, S> firstValue) {
			if (firstValue.state != null) {
				return new CrdtAccumulator<>(new CrdtData<>(firstValue.key, firstValue.state), firstValue.timestamp, 0);
			}
			return new CrdtAccumulator<>(null, 0, firstValue.timestamp);
		}

		@Override
		public CrdtAccumulator<K, S> onNextItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtReducingData<K, S> nextValue, CrdtAccumulator<K, S> accumulator) {
			if (nextValue.state != null) {
				CrdtData<K, S> next = new CrdtData<>(nextValue.key, nextValue.state);
				accumulator.accumulated = accumulator.accumulated != null ?
						combiner.apply(accumulator.accumulated, next) :
						next;
				if (nextValue.timestamp > accumulator.maxAppendTimestamp) {
					accumulator.maxAppendTimestamp = nextValue.timestamp;
				}
			} else if (nextValue.timestamp > accumulator.maxRemoveTimestamp) {
				accumulator.maxRemoveTimestamp = nextValue.timestamp;
			}
			return accumulator;
		}

		@Override
		public void onComplete(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtAccumulator<K, S> accumulator) {
			if (accumulator.accumulated != null && accumulator.maxRemoveTimestamp < accumulator.maxAppendTimestamp) {
				stream.accept(accumulator.accumulated);
			}
		}
	}

	static class FileLists {
		final List<FileMetadata> files;
		final List<FileMetadata> tombstones;

		FileLists(List<FileMetadata> files, List<FileMetadata> tombstones) {
			this.files = files;
			this.tombstones = tombstones;
		}
	}

	// region JMX
	@JmxOperation
	public void startDetailedMonitoring() {
		detailedStats = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailedStats = false;
	}

	@JmxAttribute
	public StreamStatsBasic getUploadStats() {
		return uploadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getUploadStatsDetailed() {
		return uploadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getDownloadStats() {
		return downloadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getDownloadStatsDetailed() {
		return downloadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getRemoveStatsDetailed() {
		return removeStatsDetailed;
	}

	@JmxAttribute
	public PromiseStats getConsolidationStats() {
		return consolidationStats;
	}
	// endregion
}
