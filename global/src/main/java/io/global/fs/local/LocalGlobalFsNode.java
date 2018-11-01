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
import io.global.common.RepoID;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.*;
import io.global.fs.local.LocalGlobalFsNode.Namespace.Repo;
import io.global.fs.transformers.FramesFromStorage;
import io.global.fs.transformers.FramesIntoStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public final class LocalGlobalFsNode implements GlobalFsNode, Initializable<LocalGlobalFsNode> {
	public static final Duration DEFAULT_LATENCY_MARGIN = Duration.ofMinutes(5);
	private static final Logger logger = LoggerFactory.getLogger(LocalGlobalFsNode.class);

	private final Set<RepoID> managedRepos = new HashSet<>();
	private final Set<GlobalPath> searchingForUpload = new HashSet<>();
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

	public LocalGlobalFsNode withManagedPubKeys(Set<RepoID> managedPubKeys) {
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
	public RawServerId getId() {
		return id;
	}

	@Override
	public Promise<SerialSupplier<DataFrame>> download(GlobalPath path, long offset, long limit) {
		Repo fs = ensureRepo(path.toRepoID());
		return fs.getMetadata(path.getPath())
				.thenCompose(meta -> {
					if (meta != null) {
						logger.trace("Found own local file at " + path + " at " + id);
						return fs.download(path.getPath(), offset, limit);
					}
					logger.trace("Did not found own file at " + path + " at " + id + ", searching...");
					return fs.ensureRepoNodes()
							.thenCompose(nodes ->
									Promises.firstSuccessful(nodes.stream().map(node -> node.getMetadata(path)
											.thenCompose(signedMeta -> {
												if (signedMeta == null) {
													return Promise.ofException(FILE_NOT_FOUND);
												}
												if (!signedMeta.verify(path.getOwner())) {
													return Promise.ofException(CANT_VERIFY_METADATA);
												}
												if (!doesCaching) {
													logger.trace("Trying to download file at " + path + " from " + node.getId() + "...");
													return node.download(path, offset, limit);
												}
												logger.trace("Trying to download and cache file at " + path + " from " + node.getId() + "...");
												return node.download(path, offset, limit)
														.thenApply(supplier -> {
															SerialSplitter<DataFrame> splitter = SerialSplitter.create(supplier);

															splitter.addOutput().set(SerialConsumer.ofPromise(fs.upload(path, offset)));

															return splitter.addOutput()
																	.getSupplier()
																	.withEndOfStream(eos -> eos
																			.both(splitter.getProcessResult())
																			.thenCompose($ ->
																					fs.pushMetadata(signedMeta)));
														});
											}))));
				})
				.whenComplete(toLogger(logger, "download", path, offset, limit));
	}

	@Override
	public Promise<SerialConsumer<DataFrame>> upload(GlobalPath path, long offset) {
		RepoID repoID = path.toRepoID();
		if (managedRepos.contains(repoID)) {
			return ensureRepo(repoID).upload(path, offset);
		}
		if (!searchingForUpload.add(path)) {
			return Promise.ofException(RECURSIVE_UPLOAD_ERROR);
		}
		Repo repo = ensureRepo(repoID);
		return repo.ensureRepoNodes()
				.thenCompose(nodes -> {
					if (!doesUploadCaching) {
						return Promises.firstSuccessful(nodes
								.stream()
								.map(node -> {
									logger.trace("Trying to upload file at {} at {}", path, node.getId());
									return node.upload(path, offset);
								}));
					}
					return Promises.firstSuccessful(nodes
							.stream()
							.map(node -> {
								logger.trace("Trying to upload and cache file at {} at {}", path, node.getId());
								return node.upload(path, offset)
										.thenApply(consumer -> {
											SerialZeroBuffer<DataFrame> buffer = new SerialZeroBuffer<>();

											SerialSplitter<DataFrame> splitter = SerialSplitter.<DataFrame>create()
													.withInput(buffer.getSupplier());

											splitter.addOutput().set(SerialConsumer.ofPromise(repo.upload(path, offset)));
											splitter.addOutput().set(consumer);

											MaterializedPromise<Void> process = splitter.startProcess();

											return buffer.getConsumer().withAcknowledgement(ack -> ack.both(process));
										});
							}));
				})
				.whenComplete(toLogger(logger, "download", path, offset));
	}

	@Override
	public Promise<List<SignedData<GlobalFsMetadata>>> list(RepoID repo, String glob) {
		return ensureRepo(repo).list(glob);
	}

	@Override
	public Promise<Void> pushMetadata(PubKey pubKey, SignedData<GlobalFsMetadata> signedMetadata) {
		GlobalFsMetadata meta = signedMetadata.getData();
		if (!signedMetadata.verify(pubKey)) {
			return Promise.ofException(CANT_VERIFY_METADATA);
		}
		return ensureRepo(RepoID.of(pubKey, meta.getLocalPath().getFs())).pushMetadata(signedMetadata);
	}

	private Repo ensureRepo(RepoID repoID) {
		return ensureNamespace(repoID.getOwner()).ensureRepo(repoID);
	}

	private Namespace ensureNamespace(PubKey owner) {
		return namespaces.computeIfAbsent(owner, Namespace::new);
	}

	public Promise<Boolean> fetch(RepoID repoID) {
		logger.trace("fetching from {}", repoID);
		return ensureRepo(repoID).fetch();
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
		private final PubKey owner;
		private final Map<String, Repo> repos = new HashMap<>();

		private final Map<RawServerId, GlobalFsNode> masterNodes = new HashMap<>();
		private long masterNodesLastDiscoveryTime;
		private final AsyncSupplier<List<GlobalFsNode>> ensureMasterNodesImpl = reuse(this::doEnsureMasterNodes);

		Namespace(PubKey owner) {
			this.owner = owner;
		}

		public Repo ensureRepo(RepoID repo) {
			return repos.computeIfAbsent(repo.getName(), $ -> new Repo(repo));
		}

		public Promise<List<GlobalFsNode>> ensureMasterNodes() {
			return ensureMasterNodesImpl.get();
		}

		private Promise<List<GlobalFsNode>> ensureNodes(AsyncSupplier<Optional<SignedData<AnnounceData>>> find, Map<RawServerId, GlobalFsNode> nodes, long discoveryTime) {
			if (discoveryTime > now.currentTimeMillis() - latencyMargin.toMillis()) {
				return Promise.of(new ArrayList<>(nodes.values()));
			}
			return find.get()
					.thenApply(announceData -> {
						if (announceData.isPresent()) {
							if (announceData.get().verify(owner)) {
								Set<RawServerId> newServerIds = announceData.get().getData().getServerIds();
								nodes.keySet().removeIf(id -> !newServerIds.contains(id));
								newServerIds.forEach(id -> nodes.computeIfAbsent(id, nodeClientFactory::create));
								nodes.remove(id);
								return new ArrayList<>(nodes.values());
							} else {
								logger.warn("received announce data with invalid signature");
							}
						}
						nodes.clear();
						return emptyList();
					});
		}

		private Promise<List<GlobalFsNode>> doEnsureMasterNodes() {
			return ensureNodes(() -> discoveryService.find(owner), masterNodes, masterNodesLastDiscoveryTime)
					.whenResult($ -> masterNodesLastDiscoveryTime = now.currentTimeMillis());
		}

		class Repo {
			private final RepoID repoID;
			private final FsClient folder;
			private final MetadataStorage metadataStorage;
			private final CheckpointStorage checkpointStorage;

			private final Map<RawServerId, GlobalFsNode> specificNodes = new HashMap<>();
			private long specificNodesLastDiscoveryTime;
			private final AsyncSupplier<List<GlobalFsNode>> ensureSpecificNodesImpl = reuse(this::doEnsureSpecificNodes);

			Repo(RepoID repoID) {
				this.repoID = repoID;
				String name = repoID.getName();
				String pubKeyStr = repoID.getOwner().asString();
				this.folder = dataFsClient.subfolder(pubKeyStr).subfolder(name);
				this.metadataStorage = new RemoteFsMetadataStorage(metadataFsClient.subfolder(pubKeyStr).subfolder(name));
				this.checkpointStorage = new RemoteFsCheckpointStorage(checkpointFsClient.subfolder(pubKeyStr).subfolder(name));
			}

			public Promise<List<GlobalFsNode>> ensureRepoNodes() {
				return ensureSpecificNodes()
						.thenCompose(specificNodes -> specificNodes.isEmpty() ? ensureMasterNodes() : Promise.of(specificNodes));
			}

			public Promise<List<GlobalFsNode>> ensureSpecificNodes() {
				return ensureSpecificNodesImpl.get();
			}

			private Promise<List<GlobalFsNode>> doEnsureSpecificNodes() {
				return ensureNodes(() -> discoveryService.find(repoID), specificNodes, specificNodesLastDiscoveryTime)
						.whenResult($ -> specificNodesLastDiscoveryTime = now.currentTimeMillis());
			}

			public Promise<Boolean> fetch() {
				return ensureRepoNodes()
						.thenCompose(nodes ->
								Promises.collectSequence(Try.reducer(false, (a, b) -> a || b),
										nodes.stream().map(node -> fetch(node).toTry())))
						.thenCompose(Promise::ofTry);
			}

			public Promise<Boolean> fetch(GlobalFsNode node) {
				checkState(!node.getId().equals(id), "Trying to fetch from itself");

				logger.trace("{} fetching from {}", repoID, node.getId());
				return node.list(repoID, "**")
						.thenCompose(files ->
								Promises.collectSequence(Try.reducer(false, (a, b) -> a || b), files.stream()
										.map(signedMeta -> {
											if (!signedMeta.verify(repoID.getOwner())) {
												logger.warn("Received metadata with signature that is not verified, skipping {}", signedMeta.getData());
												return Promise.of(false).toTry();
											}
											GlobalFsMetadata meta = signedMeta.getData();
											String fileName = meta.getLocalPath().getPath();
											return folder.getMetadata(fileName)
													.thenCompose(ourFileMeta -> {
														GlobalFsMetadata ourFile = into(ourFileMeta);
														// skip if our file is better
														// ourFile can be null, but meta - can't,
														// so if ourFile is null then the condition below is false
														if (GlobalFsMetadata.getBetter(ourFile, meta) == ourFile) {
															return Promise.of(false);
														}

														logger.trace(ourFile != null ?
																		"Found better metadata at {}, {}" :
																		"Found a new metadata at {}, {}",
																node.getId(), meta);

														if (meta.isRemoved()) {
															logger.trace("Removing our file as remote tombstone had bigger revision {}", ourFile);
															return folder.delete(meta.getLocalPath().getPath())
																	.thenCompose($ -> pushMetadata(signedMeta))
																	.thenApply($ -> true);
														}

														long ourSize = ourFileMeta != null ? ourFileMeta.getSize() : 0;
														long partSize = meta.getSize() - ourSize;
														GlobalPath path = GlobalPath.of(repoID.getOwner(), meta.getLocalPath().getFs(), fileName);

														// meta is newer but our size is bigger - someone truncated his file?
														if (partSize <= 0) {
															return node.download(path, meta.getSize(), 0)
																	.thenCompose(supplier -> supplier.toCollector(toList()))
																	// making sure that last frame is a checkpoint
																	.thenCompose(frames -> {
																		assert frames.size() == 1;
																		return upload(path, meta.getSize())
																				.thenCompose(SerialSupplier.of(frames.get(0))::streamTo);
																	})
																	.thenCompose($ -> pushMetadata(signedMeta))
																	.thenApply($ -> true);
														}

														// download missing part of the whole file
														return downloadMissingPart(node, fileName, ourSize, partSize, path)
																.thenCompose($ -> pushMetadata(signedMeta))
																.thenApply($ -> true);
													})
													.toTry();
										})))
						.thenCompose(Promise::ofTry);
			}

			private Promise<Void> downloadMissingPart(GlobalFsNode node, String fileName, long ourSize, long partSize, GlobalPath path) {
				if (ourSize == 0) {
					return node.download(path, ourSize, partSize)
							.thenCompose(supplier ->
									upload(path, ourSize)
											.thenCompose(supplier::streamTo));
				}
				return node.download(path, ourSize, 0)
						.thenCompose(supplier -> supplier.toCollector(toList()))
						.thenCompose(frames -> {
							// either we landed right on an existing checkpoint, then we receive just it
							if (frames.size() == 1) {
								return node.download(path, ourSize, partSize)
										.thenCompose(supplier ->
												upload(path, ourSize)
														.thenCompose(supplier::streamTo));
							}
							// or we landed in between two and received three frames - checkpoint, buf and checkpoint
							assert frames.size() == 3;
							DataFrame firstFrame = frames.get(0);
							DataFrame secondFrame = frames.get(1);
							DataFrame thirdFrame = frames.get(2);

							return download(fileName, ourSize, 0)
									.thenCompose(supplier -> supplier.toCollector(toList()))
									.thenCompose(frames2 -> {
										// here we MUST land on a checkpoint or everything is broken
										assert frames2.size() == 1;

										DataFrame ourLastFrame = frames2.get(0);

										GlobalFsCheckpoint before = firstFrame.getCheckpoint().getData();

										ByteBuf buf = secondFrame.getBuf();
										int delta = (int) (ourSize - before.getPosition());

										buf.moveReadPosition(delta);

										return node.download(path, thirdFrame.getCheckpoint().getData().getPosition(), partSize - buf.readRemaining())
												.thenCompose(supplier ->
														upload(path, ourSize)
																.thenCompose(SerialSuppliers.concat(
																		SerialSupplier.of(ourLastFrame, DataFrame.of(buf)),
																		supplier
																)::streamTo));
									});
						});
			}

			public Promise<SerialConsumer<DataFrame>> upload(GlobalPath path, long offset) {
				logger.trace("uploading to local storage {}, offset: {}", path, offset);
				return folder.upload(path.getPath(), offset)
						.thenApply(consumer -> consumer.apply(new FramesIntoStorage(path.toLocalPath(), repoID.getOwner(), checkpointStorage)));
			}

			public Promise<SerialSupplier<DataFrame>> download(String fileName, long offset, long length) {
				logger.trace("downloading local copy of {} at {}, offset: {}, length: {}", fileName, repoID, offset, length);
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
			private GlobalFsMetadata into(@Nullable FileMetadata meta) {
				if (meta == null) {
					return null;
				}
				return GlobalFsMetadata.of(LocalPath.of(repoID.getName(), meta.getName()), meta.getSize(), meta.getTimestamp());
			}
		}
	}
}
