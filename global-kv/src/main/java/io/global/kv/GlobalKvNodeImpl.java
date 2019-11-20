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

package io.global.kv;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.common.Initializable;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelOutput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.RetryPolicy;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.kv.GlobalKvNamespace.Repo;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.RawKvItem;
import io.global.kv.api.StorageFactory;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.global.util.Utils.tolerantCollectVoid;
import static io.global.util.Utils.untilTrue;

public final class GlobalKvNodeImpl extends AbstractGlobalNode<GlobalKvNodeImpl, GlobalKvNamespace, GlobalKvNode> implements GlobalKvNode, Initializable<GlobalKvNodeImpl> {
	public static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicy.immediateRetry().withMaxTotalRetryCount(10);
	public static final Duration DEFAULT_SYNC_MARGIN = Duration.ofMinutes(5);

	private Duration syncMargin = DEFAULT_SYNC_MARGIN;
	private final StorageFactory storageFactory;
	RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;

	// region creators
	private GlobalKvNodeImpl(RawServerId id, DiscoveryService discoveryService,
			Function<RawServerId, GlobalKvNode> nodeFactory,
			StorageFactory storageFactory) {
		super(id, discoveryService, nodeFactory);
		this.storageFactory = storageFactory;
	}

	public static GlobalKvNodeImpl create(RawServerId id, DiscoveryService discoveryService,
			Function<RawServerId, GlobalKvNode> nodeFactory,
			StorageFactory storageFactory) {
		return new GlobalKvNodeImpl(id, discoveryService, nodeFactory, storageFactory);
	}

	public GlobalKvNodeImpl withSyncMargin(Duration syncMargin) {
		this.syncMargin = syncMargin;
		return this;
	}

	public GlobalKvNodeImpl withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}

	// endregion

	@Override
	protected GlobalKvNamespace createNamespace(PubKey space) {
		return new GlobalKvNamespace(this, space);
	}

	public Duration getSyncMargin() {
		return syncMargin;
	}

	public StorageFactory getStorageFactory() {
		return storageFactory;
	}

	@Override
	public Promise<ChannelConsumer<SignedData<RawKvItem>>> upload(PubKey space, String table) {
		GlobalKvNamespace ns = ensureNamespace(space);
		return ns.ensureRepository(table)
				.then(repo -> repo.upload()
						.map(consumer -> consumer
								.withAcknowledgement(ack -> ack
										.whenComplete(() -> {
											if (!isMasterFor(space)) {
												repo.push();
											}
										}))));
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawKvItem>>> download(PubKey space, String table, long timestamp) {
		GlobalKvNamespace ns = ensureNamespace(space);
		return ns.ensureRepository(table)
				.then(repo -> repo.storage.download(timestamp)
						.then(supplier -> {
							if (supplier != null) {
								return Promise.of(supplier);
							}
							return simpleMethod(space,
									master -> master.download(space, table, timestamp)
											.map(itemSupplier -> {
												ChannelSplitter<SignedData<RawKvItem>> splitter = ChannelSplitter.create();
												ChannelOutput<SignedData<RawKvItem>> output = splitter.addOutput();
												splitter.addOutput().set(ChannelConsumer.ofPromise(repo.upload()));
												splitter.getInput().set(itemSupplier);
												return output.getSupplier();
											}),
									$ -> Promise.of(ChannelSupplier.of()));
						}));
	}

	@Override
	public Promise<@Nullable SignedData<RawKvItem>> get(PubKey space, String table, byte[] key) {
		GlobalKvNamespace ns = ensureNamespace(space);
		return ns.ensureRepository(table)
				.then(repo -> repo.storage.get(key)
						.then(signedData -> {
							if (signedData != null) {
								return Promise.of(signedData);
							}
							return simpleMethod(space,
									master -> master.get(space, table, key),
									$ -> Promise.of(signedData));
						}));
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		return Promise.of(ensureNamespace(space).getRepos().keySet());
	}

	public Promise<Void> fetch() {
		return Promises.all(namespaces.values().stream().map(GlobalKvNamespace::updateRepositories))
				.then($ -> forEachRepo(Repo::fetch));
	}

	public Promise<Void> push() {
		return forEachRepo(Repo::push);
	}

	private final AsyncSupplier<Void> catchUp = reuse(this::doCatchUp);

	public Promise<Void> catchUp() {
		return catchUp.get();
	}

	private Promise<Void> doCatchUp() {
		return untilTrue(() -> {
			long timestampBegin = now.currentTimeMillis();
			return fetch()
					.map($2 -> now.currentTimeMillis() <= timestampBegin + latencyMargin.toMillis());
		});
	}

	private Promise<Void> forEachRepo(Function<Repo, Promise<Void>> fn) {
		return tolerantCollectVoid(namespaces.values().stream().flatMap(entry -> entry.getRepos().values().stream()), fn);
	}

	@Override
	public String toString() {
		return "GlobalKvNodeImpl{id=" + id + '}';
	}
}
