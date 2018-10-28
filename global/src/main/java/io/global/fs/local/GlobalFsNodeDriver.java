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
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
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
import io.global.common.api.DiscoveryService;
import io.global.fs.api.*;
import io.global.fs.transformers.FramesFromStorage;
import io.global.fs.transformers.FramesIntoStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.file.FileUtils.escapeGlob;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

public final class GlobalFsNodeDriver implements GlobalFsNode, Initializable<GlobalFsNodeDriver> {
	public static final Duration DEFAULT_LATENCY_MARGIN = Duration.ofMinutes(5);
	private static final Logger logger = LoggerFactory.getLogger(GlobalFsNodeDriver.class);

	private final Set<PubKey> managedPubKeys = new HashSet<>();
	private final Set<GlobalPath> searchingForUpload = new HashSet<>();
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
	private GlobalFsNodeDriver(RawServerId id, DiscoveryService discoveryService, NodeFactory nodeFactory,
			FsClient dataFsClient, FsClient metadataFsClient, FsClient checkpointFsClient) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.nodeFactory = nodeFactory;
		this.dataFsClient = dataFsClient;
		this.metadataFsClient = metadataFsClient;
		this.checkpointFsClient = checkpointFsClient;
	}

	public static GlobalFsNodeDriver create(RawServerId id, DiscoveryService discoveryService, NodeFactory nodeFactory, FsClient fsClient) {
		return new GlobalFsNodeDriver(id, discoveryService, nodeFactory,
				fsClient.subfolder("data"), fsClient.subfolder("metadata"), fsClient.subfolder("checkpoints"));
	}

	public GlobalFsNodeDriver withManagedPubKeys(Set<PubKey> managedPubKeys) {
		this.managedPubKeys.addAll(managedPubKeys);
		return this;
	}

	public GlobalFsNodeDriver withDownloadCaching(boolean caching) {
		this.doesCaching = caching;
		return this;
	}

	public GlobalFsNodeDriver withUploadCaching(boolean caching) {
		this.doesUploadCaching = caching;
		return this;
	}

	public GlobalFsNodeDriver withoutCaching() {
		return withDownloadCaching(false);
	}

	public GlobalFsNodeDriver withLatencyMargin(Duration latencyMargin) {
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
		Namespace.Filesystem fs = ensureFilesystem(path.toRepoID());
		return fs.download(path.getPath(), offset, limit)
				.thenComposeEx((result, e) -> {
					if (e == null) {
						logger.trace("Found own local file at " + path + " at " + id);
						return Promise.of(result);
					}
					logger.trace("Did not found own file at " + path + " at " + id + ", searching...", e);
					return fs.ensureMasterNodes()
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
				});
	}

	@Override
	public Promise<SerialConsumer<DataFrame>> upload(GlobalPath path, long offset) {
		if (managedPubKeys.contains(path.getOwner())) {
			return ensureFilesystem(path.toRepoID()).upload(path, offset);
		}
		if (!searchingForUpload.add(path)) {
			return Promise.ofException(RECURSIVE_UPLOAD_ERROR);
		}
		Namespace.Filesystem fs = ensureFilesystem(path.toRepoID());
		return fs.ensureMasterNodes()
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

											splitter.addOutput().set(SerialConsumer.ofPromise(fs.upload(path, offset)));
											splitter.addOutput().set(consumer);

											MaterializedPromise<Void> process = splitter.startProcess();

											return buffer.getConsumer().withAcknowledgement(ack -> ack.both(process));
										});
							}));
				});
	}

	@Override
	public Promise<List<SignedData<GlobalFsMetadata>>> list(RepoID space, String glob) {
		return ensureFilesystem(space).list(glob);
	}

	@Override
	public Promise<Void> pushMetadata(PubKey pubKey, SignedData<GlobalFsMetadata> signedMetadata) {
		GlobalFsMetadata meta = signedMetadata.getData();
		if (!signedMetadata.verify(pubKey)) {
			return Promise.ofException(CANT_VERIFY_METADATA);
		}
		return ensureFilesystem(RepoID.of(pubKey, meta.getLocalPath().getFs())).pushMetadata(signedMetadata);
	}

	private Namespace ensureNamespace(PubKey key) {
		return publicKeyGroups.computeIfAbsent(key, GlobalFsNodeDriver.Namespace::new);
	}

	private Namespace.Filesystem ensureFilesystem(RepoID space) {
		return ensureNamespace(space.getOwner()).ensureFilesystem(space);
	}

	public Promise<Boolean> fetch(PubKey pubKey) {
		logger.trace("Fetching from {}", pubKey);
		return ensureNamespace(pubKey).fetch();
	}

	public Promise<Boolean> fetchManaged() {
		return Promises.collectSequence(Try.reducer(false, (a, b) -> a || b), managedPubKeys.stream().map(pk -> fetch(pk).toTry()))
				.thenCompose(Promise::ofTry);
	}

	class Namespace {
		private final PubKey owner;
		private final Map<RepoID, Filesystem> fileSystems = new HashMap<>();

		private final AsyncSupplier<Boolean> fetchImpl = reuse(this::doFetch);

		Namespace(PubKey owner) {
			this.owner = owner;
		}

		public Filesystem ensureFilesystem(RepoID repo) {
			return fileSystems.computeIfAbsent(repo, Filesystem::new);
		}

		public Promise<Boolean> fetch() {
			return fetchImpl.get();
		}

		private Promise<Boolean> doFetch() {
			return Promises.collectSequence(Try.reducer(false, (a, b) -> a || b), fileSystems.values().stream().map(fs -> fs.fetch().toTry()))
					.thenCompose(Promise::ofTry);
		}

		class Filesystem {
			private final RepoID repoID;
			private final FsClient folder;
			private final FsClient metadataFolder;
			private final CheckpointStorage checkpointStorage;
			private final Map<RawServerId, GlobalFsNode> masterNodes = new HashMap<>();

			private final AsyncSupplier<Void> catchUpImpl = reuse(() -> Promise.ofCallback(this::catchUpIteration));
			private final AsyncSupplier<List<GlobalFsNode>> ensureMasterNodesImpl = reuse(this::doEnsureMasterNodes);

			private long masterNodesLastDiscoveryTime;

			CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

			// region creators
			Filesystem(RepoID repoID) {
				this.repoID = repoID;
				String name = repoID.getName();
				String pubKeyStr = owner.asString();
				this.folder = dataFsClient.subfolder(pubKeyStr).subfolder(name);
				this.metadataFolder = metadataFsClient.subfolder(pubKeyStr).subfolder(name);
				this.checkpointStorage = new RemoteFsCheckpointStorage(checkpointFsClient.subfolder(pubKeyStr).subfolder(name));
			}
			// endregion

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

			public Promise<List<GlobalFsNode>> ensureMasterNodes() {
				return ensureMasterNodesImpl.get();
			}

			private Promise<List<GlobalFsNode>> doEnsureMasterNodes() {
				if (masterNodesLastDiscoveryTime >= now.currentTimeMillis() - latencyMargin.toMillis()) {
					return Promise.of(new ArrayList<>(masterNodes.values()));
				}
				return discoveryService.find(repoID)
						.thenApply(announceData -> {
							announceData.ifPresent(signedData -> {
								if (signedData.verify(owner)) {
									Set<RawServerId> newServerIds = signedData.getData().getServerIds();
									masterNodes.keySet().removeIf(id -> !newServerIds.contains(id));
									newServerIds.forEach(id -> masterNodes.computeIfAbsent(id, nodeFactory::create));
									masterNodes.remove(id);
									masterNodesLastDiscoveryTime = now.currentTimeMillis();
								}
							});
							return new ArrayList<>(masterNodes.values());
						});
			}

			public Promise<Boolean> fetch() {
				return ensureMasterNodes()
						.thenCompose(nodes ->
								Promises.collectSequence(Try.reducer(false, (a, b) -> a || b), nodes.stream()
										.map(node -> fetch(node).toTry())))
						.thenCompose(Promise::ofTry);
			}

			public Promise<Boolean> fetch(GlobalFsNode node) {
				checkState(node != GlobalFsNodeDriver.this, "Trying to fetch from itself");

				logger.trace("{} fetching from {}", repoID, node.getId());
				return node.list(repoID, "**")
						.thenCompose(files ->
								Promises.collectSequence(Try.reducer(false, (a, b) -> a || b), files.stream()
										.map(signedMeta -> {
											if (!signedMeta.verify(owner)) {
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
														GlobalPath path = GlobalPath.of(owner, meta.getLocalPath().getFs(), fileName);

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
										// here we MUST have been landed on an checkpoint, or everything is broken
										assert frames.size() == 1;
										DataFrame ourLastFrame = frames2.get(0);
										GlobalFsCheckpoint before = firstFrame.getCheckpoint().getData();
										ByteBuf buf = secondFrame.getBuf();
										int delta = (int) (ourSize - before.getPosition());
										buf.moveReadPosition(delta);
										return node.download(path, thirdFrame.getCheckpoint().getData().getPosition(), partSize - delta)
												.thenCompose(supplier ->
														upload(path, ourSize)
																.thenCompose(SerialSuppliers.concat(
																		SerialSupplier.of(ourLastFrame, DataFrame.of(buf), thirdFrame),
																		supplier
																)::streamTo));
									});
						});
			}

			public Promise<SerialConsumer<DataFrame>> upload(GlobalPath path, long offset) {
				logger.info("Uploading to local storage {}, offset: {}", path, offset);
				return folder.upload(path.getPath(), offset)
						.thenApply(consumer -> consumer.apply(new FramesIntoStorage(path.toLocalPath(), owner, checkpointStorage)));
			}

			public Promise<SerialSupplier<DataFrame>> download(String fileName, long offset, long length) {
				logger.info("Downloading local copy of {} at {}, offset: {}, length: {}", fileName, repoID, offset, length);
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
				return metadataFolder.list(glob)
						.thenCompose(res ->
								Promises.collectSequence(toList(), res.stream().map(metameta ->
										metadataFolder
												.download(metameta.getName())
												.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
												.thenCompose(buf -> {
													try {
														return Promise.of(SignedData.ofBytes(buf.asArray(), GlobalFsMetadata::fromBytes));
													} catch (ParseException e) {
														return Promise.ofException(e);
													}
												}))));
			}

			public Promise<Void> pushMetadata(SignedData<GlobalFsMetadata> metadata) {
				logger.trace("Pushing {}", metadata);
				String path = metadata.getData().getLocalPath().getPath();
				return metadataFolder.delete(escapeGlob(path))
						.thenCompose($ -> metadataFolder.upload(path, 0)) // offset 0 because atst this same file could be fetched from another node too
						.thenCompose(SerialSupplier.of(ByteBuf.wrapForReading(metadata.toBytes()))::streamTo);
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
