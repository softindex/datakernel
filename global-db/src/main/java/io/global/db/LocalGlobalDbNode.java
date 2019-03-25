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
import io.global.common.api.DiscoveryService;
import io.global.common.api.LocalGlobalNode;
import io.global.db.LocalGlobalDbNamespace.Repo;
import io.global.db.api.DbStorage;
import io.global.db.api.GlobalDbNode;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.global.util.Utils.nSuccessesOrLess;

public final class LocalGlobalDbNode extends LocalGlobalNode<LocalGlobalDbNode, LocalGlobalDbNamespace, GlobalDbNode> implements GlobalDbNode, Initializable<LocalGlobalDbNode> {
	public static final Duration DEFAULT_LATENCY_MARGIN = ApplicationSettings.getDuration(LocalGlobalDbNode.class, "latencyMargin", Duration.ofMinutes(5));

	private int uploadCallNumber = 1;
	private int uploadSuccessNumber = 0;

	private boolean doesDownloadCaching = true;
	private boolean doesUploadCaching = false;

	private Duration latencyMargin = DEFAULT_LATENCY_MARGIN;

	private final BiFunction<PubKey, String, DbStorage> storageFactory;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	// region creators
	private LocalGlobalDbNode(RawServerId id, DiscoveryService discoveryService,
							  Function<RawServerId, GlobalDbNode> nodeFactory,
							  BiFunction<PubKey, String, DbStorage> storageFactory) {
		super(id, discoveryService, nodeFactory, LocalGlobalDbNamespace::new);
		this.storageFactory = storageFactory;
	}

	public static LocalGlobalDbNode create(RawServerId id, DiscoveryService discoveryService,
										   Function<RawServerId, GlobalDbNode> nodeFactory,
										   BiFunction<PubKey, String, DbStorage> storageFactory) {
		return new LocalGlobalDbNode(id, discoveryService, nodeFactory, storageFactory);
	}
	// endregion


	public BiFunction<PubKey, String, DbStorage> getStorageFactory() {
		return storageFactory;
	}

	@Override
	public Promise<ChannelConsumer<SignedData<DbItem>>> upload(PubKey space, String table) {
		LocalGlobalDbNamespace ns = ensureNamespace(space);
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
		LocalGlobalDbNamespace ns = ensureNamespace(space);
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
		LocalGlobalDbNamespace ns = ensureNamespace(space);
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
		LocalGlobalDbNamespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.then(masters -> {
					if (isMasterFor(space)) {
						return Promise.of(new ArrayList<>(ns.getRepoNames()));
					}
					return Promises.firstSuccessful(masters.stream()
							.map(node -> AsyncSupplier.cast(() -> node.list(space))));
				});
	}

	public Promise<Void> fetch() {
		return Promises.all(getManagedPublicKeys().stream().map(this::fetch));
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
}
