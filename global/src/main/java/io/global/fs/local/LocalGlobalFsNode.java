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

package io.global.fs.local;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.functional.Try;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialSuppliers;
import io.datakernel.serial.SerialZeroBuffer;
import io.datakernel.serial.processor.SerialSplitter;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.Initializable;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.*;
import io.global.fs.transformers.FramesFromStorage;
import io.global.fs.transformers.FramesIntoStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;

public final class LocalGlobalFsNode implements GlobalFsNode, Initializable<LocalGlobalFsNode> {
	public static final Duration DEFAULT_LATENCY_MARGIN = Duration.ofMinutes(5);
	private static final Logger logger = LoggerFactory.getLogger(LocalGlobalFsNode.class);

	private final Set<PubKey> managedRepos = new HashSet<>();
	private boolean doesCaching = true;
	private boolean doesUploadCaching = false;

	private final Map<PubKey, Namespace> namespaces = new HashMap<>();

	private Duration latencyMargin = DEFAULT_LATENCY_MARGIN;

	private final RawServerId id;
	private final DiscoveryService discoveryService;
	private final NodeClientFactory nodeClientFactory;
	private final FsClient dataFsClient;
	private final FsClient metadataFsClient;
	private final FsClient checkpointFsClient;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	// region creators
	private LocalGlobalFsNode(RawServerId id, DiscoveryService discoveryService, NodeClientFactory nodeClientFactory,
			FsClient dataFsClient, FsClient metadataFsClient, FsClient checkpointFsClient) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.nodeClientFactory = nodeClientFactory;
		this.dataFsClient = dataFsClient;
		this.metadataFsClient = metadataFsClient;
		this.checkpointFsClient = checkpointFsClient;
	}

	public static LocalGlobalFsNode create(RawServerId id, DiscoveryService discoveryService, NodeClientFactory nodeClientFactory, FsClient fsClient) {
		return new LocalGlobalFsNode(id, discoveryService, nodeClientFactory,
				fsClient.subfolder("data"), fsClient.subfolder("metadata"), fsClient.subfolder("checkpoints"));
	}

	public LocalGlobalFsNode withManagedPubKeys(Set<PubKey> managedPubKeys) {
		this.managedRepos.addAll(managedPubKeys);
		return this;
	}

	public LocalGlobalFsNode withDownloadCaching(boolean caching) {
		this.doesCaching = caching;
		return this;
	}

	public LocalGlobalFsNode withUploadCaching(boolean caching) {
		this.doesUploadCaching = caching;
		return this;
	}

	public LocalGlobalFsNode withoutCaching() {
		return withDownloadCaching(false);
	}

	public LocalGlobalFsNode withLatencyMargin(Duration latencyMargin) {
		this.latencyMargin = latencyMargin;
		return this;
	}
	// endregion

	@Override
	public Promise<SerialSupplier<DataFrame>> download(PubKey space, String filename, long offset, long limit) {
		Namespace fs = ensureNamespace(space);
		return fs.getMetadata(filename)
				.thenCompose(meta -> {
					if (meta != null) {
						logger.trace("Found own local file at " + filename + " at " + id);
						return fs.load(filename, offset, limit);
					}
					logger.trace("Did not found own file at " + filename + " at " + id + ", searching...");
					return fs.ensureMasterNodes()
							.thenCompose(nodes -> Promises.firstSuccessful(
									nodes.stream()
											.map(node -> node.getMetadata(space, filename)
													.thenCompose(signedMeta -> {
														if (signedMeta == null) {
															return Promise.ofException(FILE_NOT_FOUND);
														}
														if (!signedMeta.verify(space)) {
															return Promise.ofException(CANT_VERIFY_METADATA);
														}
														if (!doesCaching) {
															logger.trace("Trying to download file at " + filename + " from " + node + "...");
															return node.download(space, filename, offset, limit);
														}
														logger.trace("Trying to download and cache file at " + filename + " from " + node + "...");
														return node.download(space, filename, offset, limit)
																.thenApply(supplier -> {
																	SerialSplitter<DataFrame> splitter = SerialSplitter.create(supplier);

																	splitter.addOutput().set(SerialConsumer.ofPromise(fs.save(filename, offset)));

																	return splitter.addOutput()
																			.getSupplier()
																			.withEndOfStream(eos -> eos
																					.both(splitter.getProcessResult())
																					.thenCompose($ ->
																							fs.pushMetadata(signedMeta)));
																});
													}))));
				})
				.whenComplete(toLogger(logger, "download", filename, offset, limit));
	}

	@Override
	public Promise<SerialConsumer<DataFrame>> upload(PubKey space, String filename, long offset) {
		if (managedRepos.contains(space)) {
			return ensureNamespace(space).save(filename, offset);
		}
//		if (!searchingForUpload.add(path)) {
//			return Promise.ofException(RECURSIVE_UPLOAD_ERROR);
//		}
		Namespace fs = ensureNamespace(space);
		return fs.ensureMasterNodes()
				.thenCompose(nodes -> {
					if (!doesUploadCaching) {
						return Promises.firstSuccessful(nodes
								.stream()
								.map(node -> {
									logger.trace("Trying to upload file at {} at {}", filename, node);
									return node.upload(space, filename, offset);
								}));
					}
					return Promises.firstSuccessful(nodes
							.stream()
							.map(node -> {
								logger.trace("Trying to upload and cache file at {} at {}", filename, node);
								return node.upload(space, filename, offset)
										.thenApply(consumer -> {
											SerialZeroBuffer<DataFrame> buffer = new SerialZeroBuffer<>();

											SerialSplitter<DataFrame> splitter = SerialSplitter.<DataFrame>create()
													.withInput(buffer.getSupplier());

											splitter.addOutput().set(SerialConsumer.ofPromise(fs.save(filename, offset)));
											splitter.addOutput().set(consumer);

											MaterializedPromise<Void> process = splitter.startProcess();

											return buffer.getConsumer().withAcknowledgement(ack -> ack.both(process));
										});
							}));
				})
				.whenComplete(toLogger(logger, "download", filename, offset));
	}

	@Override
	public Promise<List<SignedData<GlobalFsMetadata>>> list(PubKey space, String glob) {
		return ensureNamespace(space).list(glob);
	}

	@Override
	public Promise<Void> pushMetadata(PubKey space, SignedData<GlobalFsMetadata> signedMetadata) {
		if (!signedMetadata.verify(space)) {
			return Promise.ofException(CANT_VERIFY_METADATA);
		}
		return ensureNamespace(space).pushMetadata(signedMetadata);
	}

	private Namespace ensureNamespace(PubKey space) {
		return namespaces.computeIfAbsent(space, Namespace::new);
	}

	public Promise<Boolean> fetch(PubKey space) {
		logger.trace("fetching from {}", space);
		return ensureNamespace(space).fetch();
	}

	public Promise<Boolean> fetch() {
		logger.info("fetching from managed repos");
		return Promises.collectSequence(Try.reducer(false, (a, b) -> a || b), managedRepos.stream().map(pk -> fetch(pk).toTry()))
				.thenCompose(Promise::ofTry);
	}

	private final AsyncSupplier<Void> catchUpImpl = reuse(() -> Promise.ofCallback(this::catchUpIteration));

	public Promise<Void> catchUp() {
		return catchUpImpl.get();
	}

	private void catchUpIteration(SettablePromise<Void> callback) {
		long started = now.currentTimeMillis();
		fetch()
				.whenResult(didAnything -> {
					long timestampEnd = now.currentTimeMillis();
					if (!didAnything || timestampEnd - started > latencyMargin.toMillis()) {
						callback.set(null);
					} else {
						catchUpIteration(callback);
					}
				});
	}

	class Namespace {
		private final PubKey space;

		private final FsClient folder;
		private final MetadataStorage metadataStorage;
		private final CheckpointStorage checkpointStorage;

		private final AsyncSupplier<Void> doRefreshAnnouncement = reuse(this::doRefreshAnnouncement);
		private long announceTimestamp = 0L;
		private Set<RawServerId> announceServerIds;

		Namespace(PubKey space) {
			this.space = space;
			String pubKeyStr = space.asString();
			this.folder = dataFsClient.subfolder(pubKeyStr);
			this.metadataStorage = new RemoteFsMetadataStorage(metadataFsClient.subfolder(pubKeyStr));
			this.checkpointStorage = new RemoteFsCheckpointStorage(checkpointFsClient.subfolder(pubKeyStr));
		}

		public Promise<Void> refreshAnnouncements() {
			return now.currentTimeMillis() <= announceTimestamp + 10000L ?
					Promise.complete() :
					doRefreshAnnouncement.get();
		}

		private Promise<Void> doRefreshAnnouncement() {
			return discoveryService.find(space)
					.thenComposeEx((announcement, e) -> {
						if (e == null && announcement.getData().getTimestamp() > announceTimestamp) {
							announceTimestamp = announcement.getData().getTimestamp();
							announceServerIds = announcement.getData().getServerIds();
						} else {
							announceServerIds = emptySet(); // TODO anton: we dont know any masters for this space, this is bad
						}
						return Promise.complete();
					})
					.toVoid();
		}

		public Promise<List<GlobalFsNode>> ensureMasterNodes() {
			return refreshAnnouncements()
					.thenApply($ -> announceServerIds
							.stream()
							.map(nodeClientFactory::create)
							.collect(toList()));
		}

		public Promise<Boolean> fetch() {
			return ensureMasterNodes()
					.thenCompose(nodes ->
							Promises.collectSequence(Try.reducer(false, (a, b) -> a || b),
									nodes.stream().map(node -> fetch(node).toTry())))
					.thenCompose(Promise::ofTry);
		}

		public Promise<Boolean> fetch(GlobalFsNode node) {
//			checkState(!node.getId().equals(id), "Trying to fetch from itself");

			logger.trace("{} fetching from {}", space, node);
			return node.list(space, "**")
					.thenCompose(files ->
							Promises.collectSequence(Try.reducer(false, (a, b) -> a || b), files.stream()
									.map(signedMetadate -> {
										if (!signedMetadate.verify(space)) {
											logger.warn("Received metadata with signature that is not verified, skipping {}", signedMetadate.getData());
											return Promise.of(false).toTry();
										}
										GlobalFsMetadata metadata = signedMetadate.getData();
										String filename = metadata.getFilename();
										return folder.getMetadata(filename)
												.thenCompose(ourFileMeta -> {
													GlobalFsMetadata ourFile = into(ourFileMeta);
													// skip if our file is better
													// ourFile can be null, but meta - can't,
													// so if ourFile is null then the condition below is false
													if (GlobalFsMetadata.getBetter(ourFile, metadata) == ourFile) {
														return Promise.of(false);
													}

													logger.trace(ourFile != null ?
																	"Found better metadata at {}, {}" :
																	"Found a new metadata at {}, {}",
															node, metadata);

													if (metadata.isRemoved()) {
														logger.trace("Removing our file as remote tombstone had bigger revision {}", ourFile);
														return folder.delete(metadata.getFilename())
																.thenCompose($ -> pushMetadata(signedMetadate))
																.thenApply($ -> true);
													}

													long ourSize = ourFileMeta != null ? ourFileMeta.getSize() : 0;
													long partSize = metadata.getSize() - ourSize;

													// meta is newer but our size is bigger - someone truncated his file?
													if (partSize <= 0) {
														return node.download(space, filename, metadata.getSize(), 0)
																.thenCompose(supplier -> supplier.toCollector(toList()))
																// making sure that last frame is a checkpoint
																.thenCompose(frames -> {
																	assert frames.size() == 1;
																	return save(filename, metadata.getSize())
																			.thenCompose(SerialSupplier.of(frames.get(0))::streamTo);
																})
																.thenCompose($ -> pushMetadata(signedMetadate))
																.thenApply($ -> true);
													}

													// download missing part of the whole file
													return downloadMissingPart(node, filename, ourSize, partSize, filename)
															.thenCompose($ -> pushMetadata(signedMetadate))
															.thenApply($ -> true);
												})
												.toTry();
									})))
					.thenCompose(Promise::ofTry);
		}

		private Promise<Void> downloadMissingPart(GlobalFsNode node, String fileName, long ourSize, long partSize, String filename) {
			if (ourSize == 0) {
				return node.download(space, filename, ourSize, partSize)
						.thenCompose(supplier ->
								save(filename, ourSize)
										.thenCompose(supplier::streamTo));
			}
			return node.download(space, filename, ourSize, 0)
					.thenCompose(supplier -> supplier.toCollector(toList()))
					.thenCompose(frames -> {
						// either we landed right on an existing checkpoint, then we receive just it
						if (frames.size() == 1) {
							return node.download(space, filename, ourSize, partSize)
									.thenCompose(supplier ->
											save(filename, ourSize)
													.thenCompose(supplier::streamTo));
						}
						// or we landed in between two and received three frames - checkpoint, buf and checkpoint
						assert frames.size() == 3;
						DataFrame firstFrame = frames.get(0);
						DataFrame secondFrame = frames.get(1);
						DataFrame thirdFrame = frames.get(2);

						return load(fileName, ourSize, 0)
								.thenCompose(supplier -> supplier.toCollector(toList()))
								.thenCompose(frames2 -> {
									// here we MUST land on a checkpoint or everything is broken
									assert frames2.size() == 1;

									DataFrame ourLastFrame = frames2.get(0);

									GlobalFsCheckpoint before = firstFrame.getCheckpoint().getData();

									ByteBuf buf = secondFrame.getBuf();
									int delta = (int) (ourSize - before.getPosition());

									buf.moveReadPosition(delta);

									return node.download(space, fileName, thirdFrame.getCheckpoint().getData().getPosition(), partSize - buf.readRemaining())
											.thenCompose(supplier ->
													save(filename, ourSize)
															.thenCompose(SerialSuppliers.concat(
																	SerialSupplier.of(ourLastFrame, DataFrame.of(buf)),
																	supplier
															)::streamTo));
								});
					});
		}

		public Promise<SerialConsumer<DataFrame>> save(String filename, long offset) {
			logger.trace("uploading to local storage {}, offset: {}", filename, offset);
			return folder.upload(filename, offset)
					.thenApply(consumer -> consumer.apply(new FramesIntoStorage(filename, space, checkpointStorage)));
		}

		public Promise<SerialSupplier<DataFrame>> load(String fileName, long offset, long length) {
			logger.trace("downloading local copy of {} at {}, offset: {}, length: {}", fileName, space, offset, length);
			return checkpointStorage.getCheckpoints(fileName)
					.thenCompose(checkpoints -> {
						assert Arrays.equals(checkpoints, Arrays.stream(checkpoints).sorted().toArray()) : "Checkpoint array must be sorted!";

						int[] extremes = GlobalFsCheckpoint.getExtremes(checkpoints, offset, length);
						int start = extremes[0];
						int finish = extremes[1];
						return folder.download(fileName, checkpoints[start], checkpoints[finish] - checkpoints[start])
								.thenApply(supplier -> supplier.apply(new FramesFromStorage(fileName, checkpointStorage, checkpoints, start, finish)));
					});
		}

		public Promise<List<SignedData<GlobalFsMetadata>>> list(String glob) {
			return metadataStorage.list(glob);
		}

		public Promise<SignedData<GlobalFsMetadata>> getMetadata(String fileName) {
			return metadataStorage.load(fileName);
		}

		public Promise<Void> pushMetadata(SignedData<GlobalFsMetadata> metadata) {
			return metadataStorage.store(metadata);
		}

		@Nullable
		private GlobalFsMetadata into(@Nullable FileMetadata metadata) {
			if (metadata == null) {
				return null;
			}
			return GlobalFsMetadata.of(metadata.getFilename(), metadata.getSize(), metadata.getTimestamp());
		}
	}
}
