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

package io.global.globalfs.local;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.functional.Try;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;
import io.datakernel.serial.processor.SerialSplitter;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.Initializable;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.*;
import io.global.globalfs.transformers.FramesFromStorage;
import io.global.globalfs.transformers.FramesIntoStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

public final class LocalGlobalFsNode implements GlobalFsNode, Initializable<LocalGlobalFsNode> {
	public static final Duration DEFAULT_LATENCY_MARGIN = Duration.ofMinutes(5);
	private static final Logger logger = LoggerFactory.getLogger(LocalGlobalFsNode.class);

	private final Set<PubKey> managedPubKeys = new HashSet<>();
	private final Set<GlobalFsPath> searchingForUpload = new HashSet<>();
	private boolean doesCaching = true;
	private boolean doesUploadCaching = false;

	private final Map<PubKey, Namespace> publicKeyGroups = new HashMap<>();

	private Duration latencyMargin = DEFAULT_LATENCY_MARGIN;

	private final RawServerId id;
	private final DiscoveryService discoveryService;
	private final NodeFactory nodeFactory;
	private final FsClient dataFsClient;
	private final FsClient metadataFsClient;
	private final FsClient checkpointFsClient;

	// region creators
	private LocalGlobalFsNode(RawServerId id, DiscoveryService discoveryService, NodeFactory nodeFactory,
			FsClient dataFsClient, FsClient metadataFsClient, FsClient checkpointFsClient) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.nodeFactory = nodeFactory;
		this.dataFsClient = dataFsClient;
		this.metadataFsClient = metadataFsClient;
		this.checkpointFsClient = checkpointFsClient;
	}

	public static LocalGlobalFsNode create(RawServerId id, DiscoveryService discoveryService, NodeFactory nodeFactory, FsClient fsClient) {
		return new LocalGlobalFsNode(id, discoveryService, nodeFactory,
				fsClient.subfolder("data"), fsClient.subfolder("metadata"), fsClient.subfolder("checkpoints"));
	}

	public LocalGlobalFsNode withManagedPubKeys(Set<PubKey> managedPubKeys) {
		this.managedPubKeys.addAll(managedPubKeys);
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
	public RawServerId getId() {
		return id;
	}

	@Override
	public Stage<SerialSupplier<DataFrame>> download(GlobalFsPath path, long offset, long limit) {
		Namespace ns = ensureNamespace(path.getPubKey());
		Namespace.Filesystem fs = ns.ensureFilesystem(path.getSpace());
		return fs.download(path.getPath(), offset, limit)
				.thenComposeEx((result, e) -> {
					if (e == null) {
						logger.trace("Found own local file at " + path + " at " + id);
						return Stage.of(result);
					}
					logger.trace("Did not found own file at " + path + " at " + id + ", searching...");
					return ns.ensureMasterNodes()
							.thenCompose(nodes ->
									Stages.firstSuccessful(nodes.stream().map(node -> node.getMetadata(path)
											.thenCompose(signedMeta -> {
												if (signedMeta == null) {
													return Stage.ofException(FILE_NOT_FOUND);
												}
												if (!signedMeta.verify(path.getPubKey())) {
													return Stage.ofException(CANT_VERIFY_METADATA);
												}
												if (!doesCaching) {
													logger.trace("Trying to download file at " + path + " from " + node.getId() + "...");
													return node.download(path, offset, limit)
															.thenApply(supplier ->
																	supplier.withEndOfStream(eos ->
																			eos.thenCompose($ ->
																					fs.pushMetadata(signedMeta))));
												}
												logger.trace("Trying to download and cache file at " + path + " from " + node.getId() + "...");
												return node.download(path, offset, limit)
														.thenApply(supplier -> {
															SerialSplitter<DataFrame> splitter = SerialSplitter.create(supplier);

															splitter.addOutput().set(SerialConsumer.ofStage(fs.upload(path.getFullPath(), offset)));

															return splitter.addOutput()
																	.getSupplier()
																	.withEndOfStream(eos -> eos
																			.both(splitter.getProcessResult())
																			.thenCompose($ ->
																					fs.pushMetadata(signedMeta)));
														});
											}))));
				});
	}

	@Override
	public Stage<SerialConsumer<DataFrame>> upload(GlobalFsPath path, long offset) {
		if (managedPubKeys.contains(path.getPubKey())) {
			return ensureFilesystem(path.getSpace()).upload(path.getFullPath(), offset);
		}
		if (!searchingForUpload.add(path)) {
			return Stage.ofException(RECURSIVE_UPLOAD_ERROR);
		}
		Namespace ns = ensureNamespace(path.getPubKey());
		return ns.ensureMasterNodes()
				.thenCompose(nodes -> {
					if (!doesUploadCaching) {
						return Stages.firstSuccessful(nodes
								.stream()
								.map(node -> {
									logger.trace("Trying to upload file at {} at {}", path, node.getId());
									return node.upload(path, offset);
								}));
					}
					Namespace.Filesystem local = ns.ensureFilesystem(path.getSpace());
					return Stages.firstSuccessful(nodes
							.stream()
							.map(node -> {
								logger.trace("Trying to upload and cache file at {} at {}", path, node.getId());
								return node.upload(path, offset)
										.thenApply(consumer -> {
											SerialZeroBuffer<DataFrame> buffer = new SerialZeroBuffer<>();

											SerialSplitter<DataFrame> splitter = SerialSplitter.<DataFrame>create()
													.withInput(buffer.getSupplier());

											splitter.addOutput().getSupplier().streamTo(SerialConsumer.ofStage(local.upload(path.getFullPath(), offset)));
											splitter.addOutput().getSupplier().streamTo(consumer);

											MaterializedStage<Void> process = splitter.startProcess();

											return buffer.getConsumer().withAcknowledgement(ack -> ack.both(process));
										});
							}));
				});
	}

	@Override
	public Stage<List<SignedData<GlobalFsMetadata>>> list(GlobalFsSpace space, String glob) {
		return ensureFilesystem(space).list(glob);
	}

	@Override
	public Stage<Void> pushMetadata(PubKey pubKey, SignedData<GlobalFsMetadata> signedMetadata) {
		GlobalFsMetadata meta = signedMetadata.getData();
		if (!signedMetadata.verify(pubKey)) {
			return Stage.ofException(CANT_VERIFY_METADATA);
		}
		return ensureFilesystem(GlobalFsSpace.of(pubKey, meta.getFs())).pushMetadata(signedMetadata);
	}

	private Namespace ensureNamespace(PubKey key) {
		return publicKeyGroups.computeIfAbsent(key, Namespace::new);
	}

	private Namespace.Filesystem ensureFilesystem(GlobalFsSpace space) {
		return ensureNamespace(space.getPubKey()).ensureFilesystem(space);
	}

	class Namespace {
		private final PubKey pubKey;
		private final Map<GlobalFsSpace, Filesystem> fileSystems = new HashMap<>();
		private final Map<RawServerId, GlobalFsNode> masterNodes = new HashMap<>();

		private final AsyncSupplier<List<GlobalFsNode>> ensureMasterNodesImpl = reuse(this::doEnsureMasterNodes);
		private final AsyncSupplier<Void> fetchImpl = reuse(this::doFetch);

		CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

		private long nodeDiscoveryTimestamp;

		Namespace(PubKey pubKey) {
			this.pubKey = pubKey;
		}

		public Filesystem ensureFilesystem(GlobalFsSpace name) {
			return fileSystems.computeIfAbsent(name, Filesystem::new);
		}

		public Stage<List<GlobalFsNode>> ensureMasterNodes() {
			return ensureMasterNodesImpl.get();
		}

		public Stage<Void> fetch() {
			return fetchImpl.get();
		}

		private Stage<List<GlobalFsNode>> doEnsureMasterNodes() {
			if (nodeDiscoveryTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
				return Stage.of(new ArrayList<>(masterNodes.values()));
			}
			return discoveryService.findServers(pubKey)
					.thenApply(announceData -> {
						if (announceData.verify(pubKey)) {
							Set<RawServerId> newServerIds = announceData.getData().getServerIds();
							masterNodes.keySet().removeIf(id -> !newServerIds.contains(id));
							newServerIds.forEach(id -> masterNodes.computeIfAbsent(id, nodeFactory::create));
							nodeDiscoveryTimestamp = now.currentTimeMillis();
						}
						return new ArrayList<>(masterNodes.values());
					});
		}

		private Stage<Void> doFetch() {
			return Stages.all(fileSystems.values().stream()
					.map(Filesystem::fetch)
					.map(Stage::toTry));
		}

		class Filesystem {
			private final GlobalFsSpace space;
			private final FsClient folder;
			private final FsClient metadataFolder;
			private final CheckpointStorage checkpointStorage;

			private final AsyncSupplier<Void> catchUpImpl = reuse(() -> Stage.ofCallback(this::catchUpIteration));

			CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

			// region creators
			Filesystem(GlobalFsSpace space) {
				this.space = space;
				String fs = space.getFs();
				String pubKeyStr = space.getPubKey().asString();
				this.folder = dataFsClient.subfolder(pubKeyStr).subfolder(fs);
				this.metadataFolder = metadataFsClient.subfolder(pubKeyStr).subfolder(fs);
				this.checkpointStorage = new RemoteFsCheckpointStorage(checkpointFsClient.subfolder(pubKeyStr).subfolder(fs));
			}
			// endregion

			public Stage<Void> catchUp() {
				return catchUpImpl.get();
			}

			private void catchUpIteration(SettableStage<Void> callback) {
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

			public Stage<Boolean> fetch() {
				return ensureMasterNodes()
						.thenCompose(nodes ->
								Stages.collect(Try.reducer(false, (a, b) -> a || b), nodes.stream()
										.map(node -> fetch(node).toTry())))
						.thenCompose(Stage::ofTry);
			}

			public Stage<Boolean> fetch(GlobalFsNode node) {
				checkState(node != LocalGlobalFsNode.this, "Trying to fetch from itself");

				return node.list(space, "**")
						.thenCompose(files ->
								Stages.collect(Try.reducer(false, (a, b) -> a || b), files.stream()
										.map(signedMeta -> {
											if (!signedMeta.verify(space.getPubKey())) {
												return Stage.of(false).toTry();
											}
											GlobalFsMetadata meta = signedMeta.getData();
											String fileName = meta.getPath();
											return folder.getMetadata(fileName)
													.thenCompose(ourFileMeta -> {
														GlobalFsMetadata ourFile = into(ourFileMeta);
														// skip if our file is better
														// ourFile can be null, but meta - can't,
														// so if ourFile is null then the condition below is false
														if (GlobalFsMetadata.getBetter(meta, ourFile) == ourFile) {
															return Stage.of(false);
														}

														if (meta.isRemoved()) {
															return folder.delete(meta.getPath())
																	.thenCompose($ -> pushMetadata(signedMeta))
																	.thenApply($ -> true);
														}

														long ourSize = ourFileMeta != null ? ourFileMeta.getSize() : 0;
														long partSize = meta.getSize() - ourSize;

														// meta is newer but our size is bigger - someone truncated his file?
														// TODO anton: when truncating make sure to remake the last checkpoint
														if (partSize <= 0) {
															return pushMetadata(signedMeta)
																	.thenApply($ -> true);
														}

														return node.download(GlobalFsPath.of(pubKey, meta.getFs(), meta.getPath()), ourSize, partSize) // download missing part or the whole file
																.thenCompose(supplier ->
																		upload(meta.getFullPath(), ourSize)
																				.thenCompose(supplier::streamTo))
																.thenCompose($ -> pushMetadata(signedMeta))
																.thenApply($ -> true);

													})
													.toTry();
										})))
						.thenCompose(Stage::ofTry);
			}

			public Stage<SerialConsumer<DataFrame>> upload(String fullPath, long offset) {
				return folder.upload(fullPath, offset)
						.thenApply(consumer -> consumer.apply(new FramesIntoStorage(fullPath, space.getPubKey(), checkpointStorage)));
			}

			public Stage<SerialSupplier<DataFrame>> download(String fileName, long offset, long length) {
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

			public Stage<List<SignedData<GlobalFsMetadata>>> list(String glob) {
				return metadataFolder.list(glob)
						.thenCompose(res ->
								Stages.collect(toList(), res.stream().map(metameta ->
										metadataFolder
												.download(metameta.getName())
												.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
												.thenCompose(buf -> {
													try {
														return Stage.of(SignedData.ofBytes(buf.asArray(), GlobalFsMetadata::fromBytes));
													} catch (ParseException e) {
														return Stage.ofException(e);
													}
												}))));
			}

			public Stage<Void> pushMetadata(SignedData<GlobalFsMetadata> metadata) {
				return metadataFolder.upload(metadata.getData().getPath())
						.thenCompose(SerialSupplier.of(ByteBuf.wrapForReading(metadata.toBytes()))::streamTo);
			}

			@Nullable
			private GlobalFsMetadata into(@Nullable FileMetadata meta) {
				if (meta == null) {
					return null;
				}
				return GlobalFsMetadata.of(space.getFs(), meta.getName(), meta.getSize(), meta.getTimestamp());
			}
		}
	}
}
