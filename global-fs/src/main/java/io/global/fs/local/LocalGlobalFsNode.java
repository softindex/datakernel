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
import io.datakernel.functional.Try;
import io.datakernel.remotefs.FsClient;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.ApplicationSettings;
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
import java.util.stream.Stream;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.common.api.AnnouncementStorage.NO_ANNOUNCEMENT;
import static io.global.fs.api.CheckpointStorage.NO_CHECKPOINT;
import static java.util.stream.Collectors.toList;

public final class LocalGlobalFsNode implements GlobalFsNode, Initializable<LocalGlobalFsNode> {
	private static final Logger logger = LoggerFactory.getLogger(LocalGlobalFsNode.class);

	public static final Duration DEFAULT_LATENCY_MARGIN = ApplicationSettings.getDuration(LocalGlobalFsNode.class, "latencyMargin", Duration.ofMinutes(5));

	private final Set<PubKey> managedPubKeys = new HashSet<>();

	private int uploadCallNumber = 1;
	private int uploadSuccessNumber = 0;

	private boolean doesDownloadCaching = true;
	private boolean doesUploadCaching = true;

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

	private static BiFunction<SignedData<GlobalFsCheckpoint>, Throwable, Promise<GlobalFsCheckpoint>> sanitizeMeta() {
		return (signedCheckpoint, e) ->
				e != null && e != NO_CHECKPOINT ?
						Promise.ofException(e) :
						Promise.of(signedCheckpoint != null ? signedCheckpoint.getValue() : null);
	}

	private Namespace ensureNamespace(PubKey space) {
		return namespaces.computeIfAbsent(space, key -> new Namespace(key, storageFactory.apply(key), checkpointStorageFactory.apply(key)));
	}

	private boolean isMasterFor(PubKey space) {
		return managedPubKeys.contains(space);
	}

	@Override
	public Promise<ChannelConsumer<DataFrame>> upload(PubKey space, String filename, long offset) {
		Namespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.thenCompose(masters -> {
					if (isMasterFor(space)) { // check only after ensureMasterNodes because it could've made us master
						return ns.save(filename, offset);
					}
					return Promises.nSuccessesOrLess(uploadCallNumber, masters
							.stream()
							.map(master -> master.upload(space, filename, offset)))
							.thenApply(consumers -> {
								ChannelZeroBuffer<DataFrame> buffer = new ChannelZeroBuffer<>();

								ChannelSplitter<DataFrame> splitter = ChannelSplitter.<DataFrame>create()
										.withInput(buffer.getSupplier())
										.lenient();

								if (doesUploadCaching || consumers.isEmpty()) {
									splitter.addOutput()
											.set(ChannelConsumer.ofPromise(ns.save(filename, offset))
													.withAcknowledgement(ack -> ack.whenException(splitter::close)));
								}

								int[] up = {consumers.size()};

								consumers.forEach(output -> splitter.addOutput()
										.set(output
												.withAcknowledgement(ack -> ack.whenException(e -> {
													if (e != null && --up[0] < uploadSuccessNumber) {
														splitter.close(e);
													}
												}))));

								MaterializedPromise<Void> process = splitter.startProcess();

								return buffer.getConsumer()
										.withAcknowledgement(ack -> ack
												.both(process)
												.thenCompose($ -> {
													if (up[0] >= uploadSuccessNumber) {
														return Promise.complete();
													}
													return Promise.ofException(new StacklessException(LocalGlobalFsNode.class, "Not enough successes"));
												}));
							});
				})
				.whenComplete(toLogger(logger, "upload", space, filename, offset, this));
	}

	@Override
	public Promise<ChannelSupplier<DataFrame>> download(PubKey space, String filename, long offset, long length) {
		Namespace ns = ensureNamespace(space);
		return ns.getMetadata(filename)
				.thenComposeEx(sanitizeMeta())
				.thenCompose(localMeta ->
						getMetadata(space, filename)
								.thenComposeEx(sanitizeMeta())
								// if we have cached file and it is same as or better than remote
								.thenCompose(remoteMeta -> localMeta != null && localMeta.compareTo(remoteMeta) >= 0 ?
										localMeta.isTombstone() ?
												Promise.ofException(FILE_NOT_FOUND) :
												ns.load(filename, offset, length) :
										remoteMeta == null || remoteMeta.isTombstone() ?
												Promise.ofException(FILE_NOT_FOUND) :
												ns.ensureMasterNodes()
														.thenCompose(nodes -> Promises.firstSuccessful(nodes.stream()
																.map(node -> AsyncSupplier.cast(() ->
																		node.download(space, filename, offset, length)
																				.thenApply(supplier -> {
																					if (!doesDownloadCaching) {
																						logger.trace("Trying to download file at " + filename + " from " + node + "...");
																						return supplier;
																					}
																					logger.trace("Trying to download and cache file at " + filename + " from " + node + "...");

																					ChannelSplitter<DataFrame> splitter = ChannelSplitter.create(supplier);

																					splitter.addOutput()
																							.set(ChannelConsumer.ofPromise(ns.save(filename, offset)));

																					return splitter.addOutput()
																							.getSupplier()
																							.withEndOfStream(eos -> eos.both(splitter.getProcessResult()));
																				})))))))
				.whenComplete(toLogger(logger, "download", space, filename, offset, length, this));
	}

	private <T> Promise<T> simpleMethod(PubKey space, Function<GlobalFsNode, Promise<T>> self, Function<Namespace, Promise<T>> local) {
		Namespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.thenCompose(nodes ->
						isMasterFor(space) ?
								local.apply(ns) :
								Promises.firstSuccessful(Stream.concat(
										nodes.stream()
												.map(globalFsNode -> AsyncSupplier.cast(() ->
														self.apply(globalFsNode))),
										Stream.generate(
												() -> AsyncSupplier.cast(() ->
														local.apply(ns)))
												.limit(1))));
	}

	@Override
	public Promise<List<SignedData<GlobalFsCheckpoint>>> list(PubKey space, String glob) {
		return simpleMethod(space, node -> node.list(space, glob), ns -> ns.list(glob))
				.whenComplete(toLogger(logger, TRACE, "list", space, glob, this));
	}

	@Override
	public Promise<SignedData<GlobalFsCheckpoint>> getMetadata(PubKey space, String filename) {
		return simpleMethod(space, node -> node.getMetadata(space, filename), ns -> ns.getMetadata(filename))
				.thenComposeEx((res, e) -> e != null ? Promise.ofException(NO_CHECKPOINT) : Promise.of(res))
				.whenComplete(toLogger(logger, TRACE, "getMetadata", space, filename, this));
	}

	@Override
	public Promise<Void> delete(PubKey space, SignedData<GlobalFsCheckpoint> tombstone) {
		return simpleMethod(space, node -> node.delete(space, tombstone), ns -> ns.drop(tombstone))
				.whenComplete(toLogger(logger, "delete", space, tombstone, this));
	}

	public Promise<Boolean> push() {
		return Promises.collectSequence(Try.reducer(false, (a, b) -> a || b),
				namespaces.values().stream()
						.map(ns -> () -> push(ns).toTry()))
				.thenCompose(Promise::ofTry)
				.whenComplete(toLogger(logger, "push", this));
	}

	public Promise<Boolean> push(PubKey space) {
		return push(ensureNamespace(space));
	}

	private Promise<Boolean> push(Namespace ns) {
		return ns.ensureMasterNodes()
				.thenCompose(nodes ->
						Promises.collectSequence(Try.reducer(false, (a, b) -> a || b),
								nodes.stream()
										.map(node -> () -> ns.push(node, "**").toTry())))
				.thenCompose(Promise::ofTry)
				.whenComplete(toLogger(logger, "push", ns.space, this));
	}

	public Promise<Boolean> fetch() {
		return Promises.collectSequence(Try.reducer(false, (a, b) -> a || b),
				managedPubKeys.stream()
						.map(pk -> () -> fetch(pk).toTry()))
				.thenCompose(Promise::ofTry)
				.whenComplete(toLogger(logger, "fetch", this));
	}

	public Promise<Boolean> fetch(PubKey space) {
		Namespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.thenCompose(nodes ->
						Promises.collectSequence(Try.reducer(false, (a, b) -> a || b),
								nodes.stream()
										.map(node -> () -> ns.fetch(node, "**").toTry())))
				.thenCompose(Promise::ofTry)
				.whenComplete(toLogger(logger, "fetch", space, this));
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

		Namespace(PubKey space, FsClient storage, CheckpointStorage checkpointStorage) {
			this.space = space;
			this.storage = storage;
			this.checkpointStorage = checkpointStorage;
		}

		Promise<List<GlobalFsNode>> ensureMasterNodes() {
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
						if (e == null) {
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
						} else if (e != NO_ANNOUNCEMENT) {
							logger.warn("discovery service error", e);
						}
						return getMasterNodes();
					});
		}

		List<GlobalFsNode> getMasterNodes() {
			return new ArrayList<>(masterNodes.values());
		}

		@SuppressWarnings("SameParameterValue")
		Promise<Boolean> push(GlobalFsNode node, String glob) {
			return list(glob)
					.thenCompose(files ->
							Promises.collectSequence(Try.reducer(false, (a, b) -> a || b),
									files.stream()
											.map(signedLocalMeta -> () -> {
												GlobalFsCheckpoint localMeta = signedLocalMeta.getValue();
												String filename = localMeta.getFilename();

												return node.getMetadata(space, filename)
														.thenComposeEx(sanitizeMeta())
														.thenComposeEx((remoteMeta, e) -> {
															if (localMeta.compareTo(remoteMeta) < 0) {
																return Promise.of(false);
															}
															if (localMeta.isTombstone()) {
																if (remoteMeta != null && remoteMeta.isTombstone()) {
																	logger.trace("both local and remote files {} are tombstones", remoteMeta.getFilename());
																	return Promise.of(false);
																} else {
																	logger.info("local file {} is a tombstone, removing remote", localMeta.getFilename());
																	return node.delete(space, signedLocalMeta)
																			.thenApply($ -> true);
																}
															} else {
																if (remoteMeta != null && remoteMeta.isTombstone()) {
																	logger.info("remote file {} is a tombstone, removing local", remoteMeta.getFilename());
																	return drop(signedLocalMeta)
																			.thenApply($ -> true);
																}
																logger.info("pushing local file {} to node {}", localMeta.getFilename(), node);
																return streamDataFrames(LocalGlobalFsNode.this, node, filename, remoteMeta != null ? remoteMeta.getPosition() : 0)
																		.thenApply($ -> true);
															}
														})
														.toTry();
											})))
					.thenCompose(Promise::ofTry)
					.whenComplete(toLogger(logger, TRACE, "push", space, node, LocalGlobalFsNode.this));
		}

		@SuppressWarnings("SameParameterValue")
		Promise<Boolean> fetch(GlobalFsNode node, String glob) {
			return node.list(space, glob)
					.thenCompose(files ->
							Promises.collectSequence(Try.reducer(false, (a, b) -> a || b),
									files.stream()
											.map(signedRemoteMeta -> () -> {
												GlobalFsCheckpoint remoteMeta = signedRemoteMeta.getValue();
												String filename = remoteMeta.getFilename();
												return getMetadata(filename)
														.thenComposeEx(sanitizeMeta())
														.thenCompose(localMeta -> {
															if (localMeta != null) {
																if (localMeta.isTombstone()) {
																	return Promise.of(false);
																}
																if (remoteMeta.isTombstone()) {
																	logger.info("remote file {} is a tombstone, removing local", remoteMeta.getFilename());
																	return drop(signedRemoteMeta).thenApply($ -> true);
																}
																// our file is better
																if (localMeta.compareTo(remoteMeta) >= 0) {
																	logger.trace("local file {} is better than remote", localMeta.getFilename());
																	return Promise.of(false);
																}
																// other file is encrypted with different key
																if (!Objects.equals(localMeta.getSimKeyHash(), remoteMeta.getSimKeyHash())) {
																	logger.trace("remote file {} is encrypted with different key, ignoring", remoteMeta.getFilename());
																	return Promise.of(false);
																}
																logger.info("remove file {} is better than local", remoteMeta.getFilename());
															} else {
																if (remoteMeta.isTombstone()) {
																	logger.trace("remote file {} is a tombstone, removing local", remoteMeta.getFilename());
																	return drop(signedRemoteMeta)
																			.thenApply($ -> true);
																}
																logger.trace("found a new file {}", remoteMeta.getFilename());
															}

															long ourSize = localMeta != null ? localMeta.getPosition() : 0;

															assert remoteMeta.getPosition() >= ourSize : "Remote meta position is cannot be less than our size at this point";

															return streamDataFrames(node, LocalGlobalFsNode.this, filename, ourSize)
																	.thenApply($ -> true);
														})
														.toTry();
											})))
					.thenCompose(Promise::ofTry)
					.whenComplete(toLogger(logger, TRACE, "fetch", space, node, LocalGlobalFsNode.this));
		}

		Promise<Void> streamDataFrames(GlobalFsNode from, GlobalFsNode to, String filename, long position) {
			// shortcut for when we need to stream the whole file
			if (position == 0) {
				return from.download(space, filename, position, -1)
						.thenCompose(supplier ->
								to.upload(space, filename, position)
										.thenCompose(supplier::streamTo));
			}
			return from.download(space, filename, position, 0)
					.thenCompose(supplier -> supplier.toCollector(toList()))
					.thenCompose(frames -> {
						// shortcut for when we landed exactly on the checkpoint
						if (frames.size() == 1) {
							return from.download(space, filename, position, -1)
									.thenCompose(supplier ->
											to.upload(space, filename, position)
													.thenCompose(supplier::streamTo));
						}
						// or we landed in between two and received some buf frames surrounded with checkpoints
						SignedData<GlobalFsCheckpoint> signedStartCheckpoint = frames.get(0).getCheckpoint();
						List<DataFrame> bufs = frames.subList(1, frames.size() - 1);
						SignedData<GlobalFsCheckpoint> signedEndCheckpoint = frames.get(frames.size() - 1).getCheckpoint();

						GlobalFsCheckpoint startCheckpoint = signedStartCheckpoint.getValue();
						GlobalFsCheckpoint endCheckpoint = signedEndCheckpoint.getValue();

						return getMetadata(filename)
								.thenCompose(signedCheckpoint -> {
									int offset = (int) (position - startCheckpoint.getPosition());

									Iterator<DataFrame> iterator = bufs.iterator();
									ByteBuf partialBuf = iterator.next().getBuf();
									while (partialBuf.readRemaining() < offset && iterator.hasNext()) {
										offset -= partialBuf.readRemaining();
										partialBuf.recycle();
										partialBuf = iterator.next().getBuf();
									}

									partialBuf.moveReadPosition(offset);
									ByteBuf finalBuf = partialBuf;

									return from.download(space, filename, endCheckpoint.getPosition(), -1)
											.thenCompose(supplier ->
													to.upload(space, filename, position)
															.thenCompose(ChannelSuppliers.concat(
																	ChannelSupplier.of(DataFrame.of(signedCheckpoint), DataFrame.of(finalBuf)),
																	ChannelSupplier.ofIterator(iterator),
																	supplier
															)::streamTo));
								});
					});
		}

		Promise<ChannelConsumer<DataFrame>> save(String filename, long offset) {
			logger.trace("uploading to local storage {}, offset: {}", filename, offset);
			return getMetadata(filename)
					.thenComposeEx(sanitizeMeta())
					.thenCompose(meta -> {
						if (meta != null && meta.isTombstone()) {
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
			return checkpointStorage.listMetaCheckpoints(glob)
					.thenCompose(list -> Promises.collectSequence(toList(),
							list.stream()
									.map(filename -> () -> checkpointStorage.loadMetaCheckpoint(filename))));
		}

		Promise<SignedData<GlobalFsCheckpoint>> getMetadata(String fileName) {
			return checkpointStorage.loadMetaCheckpoint(fileName);
		}
	}
}
