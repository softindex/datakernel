/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalfs.local;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.exception.StacklessException;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.SerialSplitter;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.*;
import io.global.globalfs.transformers.FramesFromStorage;
import io.global.globalfs.transformers.FramesIntoStorage;

import java.time.Duration;
import java.util.*;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

public final class LocalGlobalFsNode implements GlobalFsNode {
	private final Map<PubKey, Namespace> publicKeyGroups = new HashMap<>();
	private final Set<GlobalFsPath> searchingForDownload = new HashSet<>();
	private final RawServerId id;
	private final DiscoveryService discoveryService;
	private final NodeFactory clientFactory;
	private final FsClient dataFsClient;
	private final FsClient checkpointFsClient;
	private final Settings settings;

	// region creators
	private LocalGlobalFsNode(RawServerId id, DiscoveryService discoveryService, NodeFactory clientFactory, FsClient dataFsClient, FsClient checkpointFsClient, Settings settings) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.clientFactory = clientFactory;
		this.dataFsClient = dataFsClient;
		this.checkpointFsClient = checkpointFsClient;
		this.settings = settings;
	}

	public static LocalGlobalFsNode create(RawServerId id, DiscoveryService discoveryService, NodeFactory nodeFactory, FsClient fsClient, Settings settings) {
		return new LocalGlobalFsNode(id, discoveryService, nodeFactory, fsClient.subfolder("data"), fsClient.subfolder("checkpoints"), settings);
	}
	// endregion

	private Namespace ensureNamespace(PubKey key) {
		return publicKeyGroups.computeIfAbsent(key, Namespace::new);
	}

	private Namespace.Filesystem ensureFilesystem(GlobalFsName name) {
		return ensureNamespace(name.getPubKey()).ensureFilesystem(name);
	}

	@Override
	public RawServerId getId() {
		return id;
	}

	public Settings getSettings() {
		return settings;
	}

	private Stage<SerialSupplier<DataFrame>> downloadFrom(GlobalFsNode node, Namespace.Filesystem local, GlobalFsPath address, long offset, long limit) {
		return node.getMetadata(address)
				.thenCompose(res -> {
					if (res == null) {
						return Stage.ofException(new StacklessException("no such file"));
					}
					return node.download(address, offset, limit);
				})
				.thenApply(supplier -> {
					SerialSplitter<DataFrame> splitter = SerialSplitter.<DataFrame>create()
							.withInput(supplier);

					splitter.addOutput()
							.set(SerialConsumer.ofStage(local.upload(address.getPath(), offset)));

					return splitter.addOutput().getSupplier();
				});
	}

	@Override
	public Stage<SerialSupplier<DataFrame>> download(GlobalFsPath address, long offset, long limit) {
		if (!searchingForDownload.add(address)) {
			return Stage.ofException(RECURSIVE_ERROR);
		}
		Namespace ns = ensureNamespace(address.getPubKey());
		Namespace.Filesystem fs = ns.ensureFilesystem(address.getGlobalFsName());
		return fs
				.download(address.getPath(), offset, limit)
				.thenComposeEx((result, e) -> {
					if (e == null) {
						searchingForDownload.remove(address);
						return Stage.of(result);
					}
					return ns.ensureNodes()
							.thenCompose(nodes ->
									Stages.firstSuccessful(nodes
											.stream()
											.map(node -> downloadFrom(node, fs, address, offset, limit))))
							.whenComplete(($, e1) -> searchingForDownload.remove(address));
				});
	}

	@Override
	public Stage<SerialConsumer<DataFrame>> upload(GlobalFsPath file, long offset) {
		return ensureFilesystem(file.getGlobalFsName()).upload(file.getPath(), offset);
	}

	@Override
	public Stage<List<GlobalFsMetadata>> list(GlobalFsName name, String glob) {
		return ensureFilesystem(name).list(glob);
	}

	@Override
	public Stage<Void> delete(GlobalFsName name, String glob) {
		return ensureFilesystem(name).delete(glob);
	}

	@Override
	public Stage<Set<String>> copy(GlobalFsName name, Map<String, String> changes) {
		return ensureFilesystem(name).copy(changes);
	}

	@Override
	public Stage<Set<String>> move(GlobalFsName name, Map<String, String> changes) {
		return ensureFilesystem(name).move(changes);
	}

	public interface Settings {
		Duration getLatencyMargin();
	}

	class Namespace {
		private final Map<GlobalFsName, Filesystem> fileSystems = new HashMap<>();
		private final PubKey pubKey;

		private final Map<RawServerId, GlobalFsNode> knownNodes = new HashMap<>();

		private final AsyncSupplier<List<GlobalFsNode>> ensureNodesImpl = reuse(this::doEnsureNodes);
		private final AsyncSupplier<Void> fetchImpl = reuse(this::doFetch);

		CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

		private long nodeDiscoveryTimestamp;

		// region creators
		Namespace(PubKey pubKey) {
			this.pubKey = pubKey;
			knownNodes.put(id, LocalGlobalFsNode.this);
		}
		// endregion

		public Filesystem ensureFilesystem(GlobalFsName name) {
			return fileSystems.computeIfAbsent(name, Filesystem::new);
		}

		public Stage<List<GlobalFsNode>> ensureNodes() {
			return ensureNodesImpl.get();
		}

		public Stage<Void> fetch() {
			return fetchImpl.get();
		}

		private Stage<List<GlobalFsNode>> doEnsureNodes() {
			if (nodeDiscoveryTimestamp >= now.currentTimeMillis() - settings.getLatencyMargin().toMillis()) {
				return Stage.of(new ArrayList<>(knownNodes.values()));
			}
			return discoveryService.findServers(pubKey)
					.thenApply(announceData -> {
						Set<RawServerId> newServerIds = announceData.getData().getServerIds();
						knownNodes.keySet().removeIf(t -> !newServerIds.contains(t));
						newServerIds.forEach(id -> knownNodes.computeIfAbsent(id, clientFactory::create));
						nodeDiscoveryTimestamp = now.currentTimeMillis();
						return new ArrayList<>(knownNodes.values());
					});
		}

		private Stage<Void> doFetch() {
			return Stages.all(fileSystems.values().stream()
					.map(Filesystem::fetch)
					.map(Stage::toTry));
		}

		class Filesystem {
			private final FsClient folder;
			private final CheckpointStorage checkpointStorage;
			private final GlobalFsName name;

			private final AsyncSupplier<Void> catchUpImpl = reuse(() -> Stage.ofCallback(this::catchUpIteration));

			CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

			// region creators
			Filesystem(GlobalFsName name) {
				String namespaceName = name.getPubKey().asString();
				this.folder = dataFsClient.subfolder(namespaceName).subfolder(name.getFsName());
				this.checkpointStorage = new RemoteFsCheckpointStorage(checkpointFsClient.subfolder(namespaceName).subfolder(name.getFsName()));
				this.name = name;
			}
			// endregion

			public Stage<Void> catchUp() {
				return catchUpImpl.get();
			}

			private void catchUpIteration(SettableStage<Void> callback) {
				long started = now.currentTimeMillis();
				fetch()
						.whenResult($ -> {
							long timestampEnd = now.currentTimeMillis();
							if (timestampEnd - started > getSettings().getLatencyMargin().toMillis()) {
								callback.set(null);
							} else {
								catchUpIteration(callback);
							}
						})
						.whenException(callback::setException);
			}

			public Stage<Void> fetch() {
				return ensureNodes().thenCompose(nodes -> Stages.firstSuccessful(nodes.stream().map(this::fetch)));
			}

			public Stage<Void> fetch(GlobalFsNode client) {
				checkState(client != LocalGlobalFsNode.this, "Trying to fetch from itself");

				boolean[] didDownloadAnything = {false};
				return client.getFileSystem(name)
						.list("**")
						.thenCompose(files ->
								Stages.runSequence(files.stream()
										.map(file -> {
											String fileName = file.getPath().getPath();
											return folder.getMetadata(fileName)
													.thenCompose(ourFileMeta -> {
														GlobalFsMetadata ourFile = into(ourFileMeta);
														// skip if our file is better
														// ourFile can be null, but file - can't,
														// so it will proceed to download a new file
														if (GlobalFsMetadata.getBetter(file, ourFile) == ourFile) {
															return Stage.of(null);
														}
														didDownloadAnything[0] = true;
														long ourSize = ourFileMeta != null ? ourFileMeta.getSize() : 0;
														return client.download(file.getPath(), ourSize, -1) // download missing part or the whole file
																.thenCompose(supplier ->
																		upload(fileName, ourSize)
																				.thenCompose(supplier::streamTo));

													});
										})))
						.thenComposeEx(($, e) ->
								e != null ?
										Stage.ofException(e) :
										didDownloadAnything[0] ?
												Stage.complete() :
												Stage.ofException(FETCH_DID_NOTHING)); // so we try the next node
			}

			public Stage<SerialConsumer<DataFrame>> upload(String fileName, long offset) {
				return folder.upload(fileName, offset)
						.thenApply(consumer -> consumer.apply(new FramesIntoStorage(name.getPubKey(), fileName, checkpointStorage)));
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

			public Stage<List<GlobalFsMetadata>> list(String glob) {
				return folder.list(glob).thenApply(res -> res.stream().map(this::into).collect(toList()));
			}

			public Stage<Void> delete(String glob) {
				return folder.delete(glob);
			}

			public Stage<Set<String>> copy(Map<String, String> changes) {
				return folder.copy(changes);
			}

			public Stage<Set<String>> move(Map<String, String> changes) {
				return folder.move(changes);
			}

			@Nullable
			private GlobalFsMetadata into(@Nullable FileMetadata meta) {
				if (meta == null) {
					return null;
				}
				return GlobalFsMetadata.of(name.addressOf(meta.getName()), meta.getSize(), meta.getTimestamp());
			}
		}
	}
}
