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

import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.exception.StacklessException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.functional.Try;
import io.datakernel.remotefs.FsClient;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.Initializable;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.api.NodeFactory;
import io.global.fs.api.CheckpointStorage;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FramesFromStorage;
import io.global.fs.transformers.FramesIntoStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.common.api.AnnouncementStorage.NO_ANNOUNCEMENT;
import static io.global.fs.api.CheckpointStorage.NO_CHECKPOINT;
import static java.util.stream.Collectors.toList;

public final class LocalGlobalFsNode implements GlobalFsNode, Initializable<LocalGlobalFsNode> {
	public static final Duration DEFAULT_LATENCY_MARGIN = Duration.ofMinutes(5);
	private static final Logger logger = LoggerFactory.getLogger(LocalGlobalFsNode.class);

	private final Set<PubKey> managedPubKeys = new HashSet<>();
	private boolean doesDownloadCaching = true;
	private boolean doesUploadCaching = false;

	private final Map<PubKey, Namespace> namespaces = new HashMap<>();

	private Duration latencyMargin = DEFAULT_LATENCY_MARGIN;

	private final RawServerId id;
	private final DiscoveryService discoveryService;
	private final NodeFactory<GlobalFsNode> nodeFactory;
	private final Function<PubKey, FsClient> storageFactory;
	private final Function<PubKey, CheckpointStorage> checkpointStorageFactory;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	// region creators
	private LocalGlobalFsNode(RawServerId id, DiscoveryService discoveryService,
			NodeFactory<GlobalFsNode> nodeFactory,
			Function<PubKey, FsClient> storageFactory,
			Function<PubKey, CheckpointStorage> checkpointStorageFactory) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.nodeFactory = nodeFactory;
		this.storageFactory = storageFactory;
		this.checkpointStorageFactory = checkpointStorageFactory;
	}

	public static LocalGlobalFsNode create(RawServerId id, DiscoveryService discoveryService,
			NodeFactory<GlobalFsNode> nodeFactory,
			Function<PubKey, FsClient> storageFactory,
			Function<PubKey, CheckpointStorage> checkpointStorageFactory) {
		return new LocalGlobalFsNode(id, discoveryService, nodeFactory, storageFactory, checkpointStorageFactory);
	}

	public static LocalGlobalFsNode create(RawServerId id, DiscoveryService discoveryService, NodeFactory<GlobalFsNode> nodeFactory, FsClient fsClient) {
		FsClient data = fsClient.subfolder("data");
		FsClient metadata = fsClient.subfolder("metadata");
		FsClient checkpoints = fsClient.subfolder("checkpoints");
		return new LocalGlobalFsNode(id, discoveryService, nodeFactory,
				key -> data.subfolder(key.asString()),
				key -> new RemoteFsCheckpointStorage(checkpoints.subfolder(key.asString())));
	}

	public LocalGlobalFsNode withManagedPubKeys(Set<PubKey> managedPubKeys) {
		this.managedPubKeys.addAll(managedPubKeys);
		return this;
	}

	public LocalGlobalFsNode withDownloadCaching(boolean caching) {
		this.doesDownloadCaching = caching;
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

	private static BiFunction<SignedData<GlobalFsCheckpoint>, Throwable, Promise<GlobalFsCheckpoint>> normalizeMeta(PubKey space) {
		return (signedMeta, e) -> {
			if (e != null) {
				return e == NO_CHECKPOINT ?
						Promise.of(null) :
						Promise.ofException(e);
			}
			if (!signedMeta.verify(space)) {
				return Promise.ofException(CANT_VERIFY_METADATA);
			}
			GlobalFsCheckpoint meta = signedMeta.getValue();
			if (meta.isTombstone()) {
				return Promise.of(null);
			}
			return Promise.of(meta);
		};
	}

	private boolean isMasterFor(PubKey space) {
		return managedPubKeys.contains(space);
	}

	@Override
	public Promise<ChannelConsumer<DataFrame>> upload(PubKey space, String filename, long offset) {
		Namespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.thenCompose(nodes -> {
					if (isMasterFor(space)) { // check only after ensureMasterNodes because it could've made us master
						return ns.save(filename, offset);
					} else if (nodes.isEmpty()) {
						logger.warn("no masters found for {}, caching {}", space, filename);
						ns.tempMasterMark = true;
						return ns.save(filename, offset);
					}
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
											ChannelZeroBuffer<DataFrame> buffer = new ChannelZeroBuffer<>();

											ChannelSplitter<DataFrame> splitter = ChannelSplitter.<DataFrame>create()
													.withInput(buffer.getSupplier());

											splitter.addOutput().set(ChannelConsumer.ofPromise(ns.save(filename, offset)));
											splitter.addOutput().set(consumer);

											MaterializedPromise<Void> process = splitter.startProcess();

											return buffer.getConsumer().withAcknowledgement(ack -> ack.both(process));
										});
							}));
				})
				.whenComplete(toLogger(logger, "upload", space, filename, offset, this));
	}

	@Override
	public Promise<ChannelSupplier<DataFrame>> download(PubKey space, String filename, long offset, long limit) {
		Namespace ns = ensureNamespace(space);
		return ns.getMetadata(filename)
				.thenComposeEx((signedLocalMeta, e) -> {
					if (e != null && e != NO_CHECKPOINT) {
						return Promise.ofException(e);
					}
					return getMetadata(space, filename)
							.thenComposeEx(normalizeMeta(space))
							.thenCompose(remoteMeta -> {
								if (signedLocalMeta != null) { // if we have cached..
									GlobalFsCheckpoint localMeta = signedLocalMeta.getValue();
									if (localMeta.compareTo(remoteMeta) >= 0) { // ..and it is same or better than remote
										return localMeta.isTombstone() ?
												Promise.ofException(FILE_NOT_FOUND) :
												ns.load(filename, offset, limit); // then we return it
									}
								}
								if (remoteMeta == null) {
									return Promise.ofException(FILE_NOT_FOUND);
								}
								return ns.ensureMasterNodes()
										.thenCompose(nodes ->
												Promises.firstSuccessful(nodes
														.stream()
														.map(node -> node.getMetadata(space, filename)
																.thenCompose(signedMeta -> {
																	if (!signedMeta.verify(space)) {
																		return Promise.ofException(CANT_VERIFY_METADATA);
																	}
																	if (!doesDownloadCaching) {
																		logger.trace("Trying to download file at " + filename + " from " + node + "...");
																		return node.download(space, filename, offset, limit);
																	}
																	logger.trace("Trying to download and cache file at " + filename + " from " + node + "...");
																	return node.download(space, filename, offset, limit)
																			.thenApply(supplier2 -> {
																				ChannelSplitter<DataFrame> splitter = ChannelSplitter.create(supplier2);

																				splitter.addOutput().set(ChannelConsumer.ofPromise(ns.save(filename, offset)));

																				return splitter.addOutput()
																						.getSupplier()
																						.withEndOfStream(eos -> eos.both(splitter.getProcessResult()));
																			});
																}))));
							});
				})
				.whenComplete(toLogger(logger, "download", space, filename, offset, limit, this));
	}

	@Override
	public Promise<List<SignedData<GlobalFsCheckpoint>>> list(PubKey space, String glob) {
		Namespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.thenCompose(nodes ->
						isMasterFor(space) || nodes.isEmpty() ?
								ns.list(glob) :
								Promises.firstSuccessful(nodes.stream()
										.map(node -> node.list(space, glob))))
				.whenComplete(toLogger(logger, "list", space, glob, this));
	}

	@Override
	public Promise<SignedData<GlobalFsCheckpoint>> getMetadata(PubKey space, String filename) {
		Namespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.thenCompose(nodes ->
						isMasterFor(space) || nodes.isEmpty() ?
								ns.getMetadata(filename) :
								Promises.firstSuccessful(nodes.stream()
										.map(node -> node.getMetadata(space, filename))))
				.whenComplete(toLogger(logger, "getMetadata", space, filename, this));
	}

	@Override
	public Promise<Void> delete(PubKey space, SignedData<GlobalFsCheckpoint> tombstone) {
		Namespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.thenCompose(nodes ->
						isMasterFor(space) || nodes.isEmpty() ?
								ns.drop(tombstone) :
								Promises.firstSuccessful(nodes.stream()
										.map(node -> node.delete(space, tombstone))))
				.whenComplete(toLogger(logger, "delete", space, tombstone, this));
	}

	private Namespace ensureNamespace(PubKey space) {
		return namespaces.computeIfAbsent(space, key -> new Namespace(key, storageFactory.apply(key), checkpointStorageFactory.apply(key)));
	}

	public Promise<Boolean> fetch(PubKey space) {
		logger.trace("fetching from {}", space);
		return ensureNamespace(space).fetch()
				.whenComplete(toLogger(logger, "fetch", space, this));
	}

	public Promise<Boolean> fetch() {
		logger.info("fetching from managed repos");
		return Promises.collectSequence(Try.reducer(false, (a, b) -> a || b), managedPubKeys.stream().map(pk -> fetch(pk).toTry()))
				.thenCompose(Promise::ofTry)
				.whenComplete(toLogger(logger, "fetch", this));
	}

	private final AsyncSupplier<Void> catchUpImpl = reuse(() -> Promise.ofCallback(this::catchUpIteration));

	public Promise<Void> catchUp() {
		return catchUpImpl.get()
				.whenComplete(toLogger(logger, "catchUp", this));
	}

	private void catchUpIteration(SettablePromise<Void> cb) {
		long started = now.currentTimeMillis();
		fetch()
				.whenResult(didAnything -> {
					long timestampEnd = now.currentTimeMillis();
					if (!didAnything || timestampEnd - started > latencyMargin.toMillis()) {
						cb.set(null);
					} else {
						catchUpIteration(cb);
					}
				});
	}

	@Override
	public String toString() {
		return "LocalGlobalFsNode{id=" + id + '}';
	}

	class Namespace {
		final PubKey space;

		final FsClient storage;
		final CheckpointStorage checkpointStorage;

		final AsyncSupplier<List<GlobalFsNode>> ensureMasterNodes = reuse(this::doEnsureMasterNodes);
		final Map<RawServerId, GlobalFsNode> masterNodes = new HashMap<>();
		long updateNodesTimestamp;
		long announceTimestamp;

		boolean tempMasterMark = false;

		Namespace(PubKey space, FsClient storage, CheckpointStorage checkpointStorage) {
			this.space = space;
			this.storage = storage;
			this.checkpointStorage = checkpointStorage;
		}

		public Promise<List<GlobalFsNode>> ensureMasterNodes() {
			return ensureMasterNodes.get();
		}

		@SuppressWarnings("Duplicates")
			// this method is just the same as in GlobalOTNodeImpl
		Promise<List<GlobalFsNode>> doEnsureMasterNodes() {
			if (updateNodesTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
				return Promise.of(getMasterNodes());
			}
			return discoveryService.find(space)
					.thenApplyEx((announceData, e) -> {
						if (e != null) {
							if (e != NO_ANNOUNCEMENT) {
								throw new UncheckedException(e);
							}
						} else if (announceData.verify(space)) {
							AnnounceData announce = announceData.getValue();
							if (announce.getTimestamp() >= announceTimestamp) {
								Set<RawServerId> newServerIds = new HashSet<>(announce.getServerIds());
								masterNodes.keySet().removeIf(id -> !newServerIds.contains(id));
								if (newServerIds.remove(id)) { // ensure that we are master for the space if it was announced
									if (managedPubKeys.add(space)) {
										logger.trace("became a master for {}: {}", space, LocalGlobalFsNode.this);
									}
								} else {
									if (managedPubKeys.remove(space)) {
										logger.trace("stopped being a master for {}: {}", space, LocalGlobalFsNode.this);
									}
								}
								newServerIds.forEach(id -> masterNodes.computeIfAbsent(id, nodeFactory::create));
								updateNodesTimestamp = now.currentTimeMillis();
								announceTimestamp = announce.getTimestamp();
							}
						}
						return getMasterNodes();
					});
		}

		public List<GlobalFsNode> getMasterNodes() {
			return new ArrayList<>(masterNodes.values());
		}

		public Promise<Boolean> fetch() {
			return ensureMasterNodes()
					.thenCompose(nodes ->
							Promises.collectSequence(Try.reducer(false, (a, b) -> a || b),
									nodes.stream().map(node -> fetch(node).toTry())))
					.thenCompose(Promise::ofTry);
		}

		public Promise<Boolean> fetch(GlobalFsNode node) {
			logger.trace("{} fetching from {}", space, node);
			return node.list(space, "**")
					.thenCompose(files ->
							Promises.collectSequence(Try.reducer(false, (a, b) -> a || b), files.stream()
									.map(signedMetadata -> {
										if (!signedMetadata.verify(space)) {
											logger.warn("received metadata with unverified signature, skipping {}", signedMetadata.getValue());
											return Promise.of(false).toTry();
										}
										GlobalFsCheckpoint metadata = signedMetadata.getValue();
										String filename = metadata.getFilename();
										return getMetadata(filename)
												.thenComposeEx((signedLocalMetadata, e) -> {
													if (e != null && e != NO_CHECKPOINT) {
														return Promise.ofException(e);
													}
													GlobalFsCheckpoint localMetadata = null;
													if (signedLocalMetadata != null) {
														if (!signedLocalMetadata.verify(space)) {
															logger.warn("found local metadata with unverified signature, skipping {}", signedLocalMetadata.getValue());
															return Promise.of(false);
														} else {
															localMetadata = signedLocalMetadata.getValue();
														}
													}

													if (localMetadata != null) {
														if (localMetadata.isTombstone()) {
															return Promise.of(false);
														}
														if (metadata.isTombstone()) {
															logger.trace("remote file {} is a tombstone", metadata.getFilename());
															return drop(signedMetadata).thenApply($ -> true);
														}
														// our file is better
														if (localMetadata.compareTo(metadata) >= 0) {
															logger.trace("our file {} is better than remote", localMetadata.getFilename());
															return Promise.of(false);
														}
														// other file is encrypted with different key
														// (first condition is because tombstones don't store key hash)
														if (!Objects.equals(localMetadata.getSimKeyHash(), metadata.getSimKeyHash())) {
															logger.trace("remote file {} is encrypted with different key, ignoring", metadata.getFilename());
															return Promise.of(false);
														}
														logger.trace("found better version of {}", metadata.getFilename());
													} else {
														if (metadata.isTombstone()) {
															logger.trace("found a new tombstone for {}", metadata.getFilename());
															return drop(signedMetadata).thenApply($ -> true);
														}
														logger.trace("found a new file {}", metadata.getFilename());
													}

													long ourSize = localMetadata != null ? localMetadata.getPosition() : 0;
													long partSize = metadata.getPosition() - ourSize;

													// meta is newer but our size is bigger - someone truncated his file?
													if (partSize < 0) {
														return Promise.ofException(new StacklessException(Namespace.class, "pushMetadata truncations are disallowed!"));
													}

													if (ourSize == 0) {
														return node.download(space, filename, 0, partSize)
																.thenCompose(supplier ->
																		save(filename, 0)
																				.thenCompose(supplier::streamTo))
																.thenApply($ -> true);
													}

													// download missing part of the whole file
													return downloadMissingPart(node, filename, ourSize, partSize, filename)
															.thenApply($ -> true);
												})
												.toTry();
									})))
					.thenCompose(Promise::ofTry);
		}

		Promise<Void> downloadMissingPart(GlobalFsNode node, String fileName, long ourSize, long partSize, String filename) {
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

									GlobalFsCheckpoint before = firstFrame.getCheckpoint().getValue();

									ByteBuf buf = secondFrame.getBuf();
									int delta = (int) (ourSize - before.getPosition());

									buf.moveReadPosition(delta);

									return node.download(space, fileName, thirdFrame.getCheckpoint().getValue().getPosition(), partSize - buf.readRemaining())
											.thenCompose(supplier ->
													save(filename, ourSize)
															.thenCompose(ChannelSuppliers.concat(
																	ChannelSupplier.of(ourLastFrame, DataFrame.of(buf)),
																	supplier
															)::streamTo));
								});
					});
		}

		Promise<ChannelConsumer<DataFrame>> save(String filename, long offset) {
			logger.trace("uploading to local storage {}, offset: {}", filename, offset);
			return getMetadata(filename)
					.thenComposeEx((checkpoint, e) -> {
						if (e != null && e != NO_CHECKPOINT) {
							return Promise.ofException(e);
						}
						if (checkpoint != null && checkpoint.getValue().isTombstone()) {
							return Promise.ofException(UPLOADING_TO_TOMBSTONE);
						}
						return storage.upload(filename, offset)
								.thenApply(consumer -> consumer.transformWith(FramesIntoStorage.create(filename, space, checkpointStorage)));
					});
		}

		Promise<ChannelSupplier<DataFrame>> load(String fileName, long offset, long length) {
			logger.trace("downloading local copy of {} at {}, offset: {}, length: {}", fileName, space, offset, length);
			return checkpointStorage.loadIndex(fileName)
					.thenCompose(checkpoints -> {
						assert Arrays.equals(checkpoints, Arrays.stream(checkpoints).sorted().toArray()) : "Checkpoint array must be sorted!";

						int[] extremes = GlobalFsCheckpoint.getExtremes(checkpoints, offset, length);
						int start = extremes[0];
						int finish = extremes[1];
						return storage.download(fileName, checkpoints[start], checkpoints[finish] - checkpoints[start])
								.thenApply(supplier -> supplier.transformWith(FramesFromStorage.create(fileName, checkpointStorage, checkpoints, start, finish)));
					});
		}

		Promise<Void> drop(SignedData<GlobalFsCheckpoint> tombstone) {
			assert tombstone.getValue().isTombstone() : "trying to drop file with non-tombstone checkpoint";
			String filename = tombstone.getValue().getFilename();
			return checkpointStorage.drop(filename)
					.thenCompose($ ->
							Promises.all(storage.delete(filename), checkpointStorage.store(filename, tombstone)));
		}

		Promise<List<SignedData<GlobalFsCheckpoint>>> list(String glob) {
			return checkpointStorage.loadLastCheckpoints(glob);
		}

		Promise<SignedData<GlobalFsCheckpoint>> getMetadata(String fileName) {
			return checkpointStorage.loadLastCheckpoint(fileName);
		}
	}
}
