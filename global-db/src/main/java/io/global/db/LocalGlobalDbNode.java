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

package io.global.db;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.SettableCallback;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelOutput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.exception.StacklessException;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.Initializable;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.api.NodeFactory;
import io.global.db.LocalGlobalDbNode.Namespace.Repo;
import io.global.db.api.DbStorage;
import io.global.db.api.GlobalDbNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.BiFunction;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.global.util.Utils.nSuccessesOrLess;

public final class LocalGlobalDbNode implements GlobalDbNode, Initializable<LocalGlobalDbNode> {
	private static final Logger logger = LoggerFactory.getLogger(LocalGlobalDbNode.class);

	public static final Duration DEFAULT_LATENCY_MARGIN = ApplicationSettings.getDuration(LocalGlobalDbNode.class, "latencyMargin", Duration.ofMinutes(5));

	private final Set<PubKey> managedPubKeys = new HashSet<>();

	private int uploadCallNumber = 1;
	private int uploadSuccessNumber = 0;

	private boolean doesDownloadCaching = true;
	private boolean doesUploadCaching = false;

	private final Map<PubKey, Namespace> namespaces = new HashMap<>();

	private Duration latencyMargin = DEFAULT_LATENCY_MARGIN;

	private final RawServerId id;
	private final DiscoveryService discoveryService;
	private final NodeFactory<GlobalDbNode> nodeFactory;
	private final BiFunction<PubKey, String, DbStorage> storageFactory;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	// region creators
	private LocalGlobalDbNode(RawServerId id, DiscoveryService discoveryService,
							  NodeFactory<GlobalDbNode> nodeFactory,
							  BiFunction<PubKey, String, DbStorage> storageFactory) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.nodeFactory = nodeFactory;
		this.storageFactory = storageFactory;
	}

	public static LocalGlobalDbNode create(RawServerId id, DiscoveryService discoveryService,
										   NodeFactory<GlobalDbNode> nodeFactory,
										   BiFunction<PubKey, String, DbStorage> storageFactory) {
		return new LocalGlobalDbNode(id, discoveryService, nodeFactory, storageFactory);
	}

	public LocalGlobalDbNode withManagedPubKey(PubKey managedPubKey) {
		managedPubKeys.add(managedPubKey);
		return this;
	}

	public LocalGlobalDbNode withManagedPubKeys(Set<PubKey> managedPubKeys) {
		this.managedPubKeys.addAll(managedPubKeys);
		return this;
	}

	public LocalGlobalDbNode withLatencyMargin(Duration latencyMargin) {
		this.latencyMargin = latencyMargin;
		return this;
	}
	// endregion

	private Namespace ensureNamespace(PubKey space) {
		return namespaces.computeIfAbsent(space, Namespace::new);
	}

	private boolean isMasterFor(PubKey space) {
		return managedPubKeys.contains(space);
	}

	@Override
	public Promise<ChannelConsumer<SignedData<DbItem>>> upload(PubKey space, String table) {
		Namespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.then(masters -> {
					Repo repo = ns.ensureRepository(table);
					if (isMasterFor(space)) {
						return repo.upload();
					}
					return nSuccessesOrLess(uploadCallNumber, masters
							.stream()
							.map(master -> AsyncSupplier.cast(() -> master.upload(space, table))))
							.map(consumers -> {
								ChannelZeroBuffer<SignedData<DbItem>> buffer = new ChannelZeroBuffer<>();

								ChannelSplitter<SignedData<DbItem>> splitter = ChannelSplitter.create(buffer.getSupplier())
										.lenient();

								boolean[] localCompleted = {false};
								if (doesUploadCaching || consumers.isEmpty()) {
									splitter.addOutput().set(ChannelConsumer.ofPromise(repo.upload())
											.withAcknowledgement(ack -> ack
													.acceptEx(($, e) -> {
														if (e == null) {
															localCompleted[0] = true;
														} else {
															splitter.close(e);
														}
													})));
								} else {
									localCompleted[0] = true;
								}

								int[] up = {consumers.size()};

								consumers.forEach(output -> splitter.addOutput()
										.set(output.withAcknowledgement(ack -> ack.acceptEx(Exception.class, e -> {
											if (e != null && --up[0] < uploadSuccessNumber && localCompleted[0]) {
												splitter.close(e);
											}
										}))));

								return buffer.getConsumer()
										.withAcknowledgement(ack -> ack
												.then($ -> {
													if (up[0] >= uploadSuccessNumber) {
														return Promise.complete();
													}
													return Promise.ofException(new StacklessException(LocalGlobalDbNode.class, "Not enough successes"));
												}));
							});

				});
	}

	@Override
	public Promise<ChannelSupplier<SignedData<DbItem>>> download(PubKey space, String table, long timestamp) {
		Namespace ns = ensureNamespace(space);
		Repo repo = ns.ensureRepository(table);
		return repo.download(timestamp)
				.then(supplier -> {
					if (supplier != null) {
						return Promise.of(supplier);
					}
					return ns.ensureMasterNodes()
							.then(masters -> {
								if (isMasterFor(space) || masters.isEmpty()) {
									return repo.storage.download(timestamp);
								}
								if (!doesDownloadCaching) {
									return Promises.firstSuccessful(masters.stream()
											.map(node -> AsyncSupplier.cast(() ->
													node.download(space, table, timestamp))));
								}
								return Promises.firstSuccessful(masters.stream()
										.map(node -> AsyncSupplier.cast(() ->
												Promises.toTuple(node.download(space, table, timestamp), repo.upload())
														.map(t -> {
															ChannelSplitter<SignedData<DbItem>> splitter = ChannelSplitter.create();
															ChannelOutput<SignedData<DbItem>> output = splitter.addOutput();
															splitter.addOutput().set(t.getValue2());
															splitter.getInput().set(t.getValue1());
															return output.getSupplier();
														}))));
							});
				});
	}

	@Override
	public Promise<SignedData<DbItem>> get(PubKey space, String table, byte[] key) {
		Namespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.then(masters -> {
					Repo repo = ns.ensureRepository(table);
					if (isMasterFor(space)) {
						return repo.storage.get(key);
					}
					return Promises.firstSuccessful(masters.stream()
							.map(node -> AsyncSupplier.cast(
									doesDownloadCaching ?
											() -> node.get(space, table, key)
													.then(item ->
															repo.storage.put(item)
																	.map($ -> item)) :
											() -> node.get(space, table, key))));
				});
	}

	@Override
	public Promise<List<String>> list(PubKey space) {
		Namespace ns = ensureNamespace(space);
		return ns
				.ensureMasterNodes()
				.then(masters -> {
					if (!isMasterFor(space)) {
						return Promises.firstSuccessful(masters.stream()
								.map(node -> AsyncSupplier.cast(() -> node.list(space))));
					}
					return Promise.of(new ArrayList<>(ns.repos.keySet()));
				});
	}

	public Promise<Void> fetch() {
		return Promises.all(managedPubKeys.stream().map(this::fetch));
	}

	public Promise<Void> push() {
		return Promises.all(namespaces.keySet().stream().map(this::push));
	}

	public Promise<Void> fetch(PubKey space) {
		return ensureNamespace(space).fetch();
	}

	public Promise<Void> push(PubKey space) {
		return ensureNamespace(space).push();
	}

	private final AsyncSupplier<Void> catchUpImpl = reuse(() -> Promise.ofCallback(this::catchUpImpl));

	public Promise<Void> catchUp() {
		return catchUpImpl.get();
	}

	private void catchUpImpl(SettableCallback<@Nullable Void> cb) {
		long started = now.currentTimeMillis();
		Promise<Void> fetchPromise = fetch();
		if (fetchPromise.isResult()) {
			cb.set(fetchPromise.materialize().getResult());
		} else if (fetchPromise.isException()) {
			cb.setException(fetchPromise.materialize().getException());
		} else {
			fetchPromise
					.accept($ -> {
						long timestampEnd = now.currentTimeMillis();
						if (timestampEnd - started > latencyMargin.toMillis()) {
							catchUpImpl(cb);
						} else {
							cb.set(null);
						}
					})
					.acceptEx(Exception.class, cb::setException);
		}
	}

	@Override
	public String toString() {
		return "LocalGlobalDbNode{id=" + id + '}';
	}

	class Namespace {
		final PubKey space;

		final Map<String, Repo> repos = new HashMap<>();

		final AsyncSupplier<List<GlobalDbNode>> ensureMasterNodes = reuse(this::doEnsureNodes);
		final Map<RawServerId, GlobalDbNode> masterNodes = new HashMap<>();
		long updateNodesTimestamp;
		long announceTimestamp;

		Namespace(PubKey space) {
			this.space = space;
		}

		Repo ensureRepository(String table) {
			return repos.computeIfAbsent(table, t -> new Repo(storageFactory.apply(space, t), space, t));
		}

		Promise<Void> fetch() {
			return ensureMasterNodes()
					.then(masters ->
							Promises.all(masters.stream()
									.map(node -> node
											.list(space)
											.then(tables ->
													Promises.all(tables.stream()
															.map(table ->
																	ensureRepository(table)
																			.fetch(node)))))));
		}

		Promise<Void> push() {
			return ensureMasterNodes()
					.then(masters ->
							Promises.all(masters
									.stream()
									.map(node ->
											Promises.all(repos
													.values()
													.stream()
													.map(repo -> repo.push(node))))));
		}

		Promise<List<GlobalDbNode>> ensureMasterNodes() {
			return ensureMasterNodes.get();
		}

		@NotNull
		Promise<List<GlobalDbNode>> doEnsureNodes() {
			if (updateNodesTimestamp >= now.currentTimeMillis() - latencyMargin.toMillis()) {
				return Promise.of(getMasterNodes());
			}
			return discoveryService.find(space)
					.mapEx((announceData, e) -> {
						if (e == null && announceData != null) {
							AnnounceData announce = announceData.getValue();
							if (announce.getTimestamp() >= announceTimestamp) {
								Set<RawServerId> newServerIds = new HashSet<>(announce.getServerIds());
								masterNodes.keySet().removeIf(id -> !newServerIds.contains(id));
								if (newServerIds.remove(id)) { // ensure that we are master for the space if it was announced
									if (managedPubKeys.add(space)) {
										logger.trace("became a master for {}: {}", space, LocalGlobalDbNode.this);
									}
								} else {
									if (managedPubKeys.remove(space)) {
										logger.trace("stopped being a master for {}: {}", space, LocalGlobalDbNode.this);
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

		@NotNull
		List<GlobalDbNode> getMasterNodes() {
			return new ArrayList<>(masterNodes.values());
		}

		class Repo {
			final DbStorage storage;
			final PubKey space;
			final String table;

			long lastFetchTimestamp = 0;
			long cacheTimestamp = 0;

			Repo(DbStorage storage, PubKey space, String table) {
				this.storage = storage;
				this.space = space;
				this.table = table;
			}

			Promise<ChannelConsumer<SignedData<DbItem>>> upload() {
				return storage.upload()
						.map(consumer -> consumer
								.withAcknowledgement(ack -> ack
										.accept($ -> cacheTimestamp = now.currentTimeMillis())));
			}

			Promise<ChannelSupplier<SignedData<DbItem>>> download(long timestamp) {
				return now.currentTimeMillis() - cacheTimestamp <= latencyMargin.toMillis() ?
						storage.download(timestamp) :
						Promise.of(null);
			}

			Promise<Void> fetch(GlobalDbNode node) {
				long timestamp = now.currentTimeMillis();
				return Promises.toTuple(node.download(space, table, lastFetchTimestamp), storage.upload())
						.then(tuple -> tuple.getValue1().streamTo(tuple.getValue2()))
						.accept($ -> lastFetchTimestamp = timestamp);
			}

			Promise<Void> push(GlobalDbNode node) {
				return Promises.toTuple(storage.download(), node.upload(space, table))
						.then(tuple -> tuple.getValue1().streamTo(tuple.getValue2()));
			}
		}
	}
}
