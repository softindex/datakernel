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
import io.global.common.api.AbstractRepoGlobalNamespace.Repo;
import io.global.common.api.AbstractRepoGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.common.api.RepoStorageFactory;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.KvStorage;
import io.global.kv.api.RawKvItem;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.global.util.Utils.tolerantCollectVoid;
import static io.global.util.Utils.untilTrue;

public final class GlobalKvNodeImpl extends AbstractRepoGlobalNode<GlobalKvNodeImpl, GlobalKvNamespace, GlobalKvNode, KvStorage> implements GlobalKvNode, Initializable<GlobalKvNodeImpl> {
	public static final RetryPolicy<?> DEFAULT_RETRY_POLICY = RetryPolicy.immediateRetry().withMaxTotalRetryCount(10);

	private final RepoStorageFactory<KvStorage> storageFactory;

	// region creators
	private GlobalKvNodeImpl(RawServerId id, DiscoveryService discoveryService,
			Function<RawServerId, GlobalKvNode> nodeFactory,
			RepoStorageFactory<KvStorage> storageFactory) {
		super(id, discoveryService, nodeFactory);
		this.storageFactory = storageFactory;
	}

	public static GlobalKvNodeImpl create(RawServerId id, DiscoveryService discoveryService,
			Function<RawServerId, GlobalKvNode> nodeFactory,
			RepoStorageFactory<KvStorage> storageFactory) {
		return new GlobalKvNodeImpl(id, discoveryService, nodeFactory, storageFactory);
	}

	public GlobalKvNodeImpl withSyncMargin(Duration latencyMargin) {
		this.latencyMargin = latencyMargin;
		return this;
	}
	// endregion

	@Override
	protected GlobalKvNamespace createNamespace(PubKey space) {
		return new GlobalKvNamespace(this, space);
	}

	public RepoStorageFactory<KvStorage> getStorageFactory() {
		return storageFactory;
	}

	public Duration getSyncMargin() {
		return latencyMargin;
	}

	@Override
	public Promise<ChannelConsumer<SignedData<RawKvItem>>> upload(PubKey space, String table) {
		GlobalKvNamespace ns = ensureNamespace(space);
		return ns.ensureRepository(table)
				.then(repo -> repo.getStorage().upload()
						.map(consumer -> consumer
								.withAcknowledgement(ack -> ack
										.whenComplete(() -> {
											if (!isMasterFor(space)) {
												repo.push();
											}
										}))));
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawKvItem>>> download(PubKey space, String repo, long timestamp) {
		return simpleRepoMethod(space, repo,
				r -> r.getStorage().download(timestamp),
				(master, r) -> master.download(space, repo, timestamp)
						.map(itemSupplier -> {
							ChannelSplitter<SignedData<RawKvItem>> splitter = ChannelSplitter.create();
							ChannelOutput<SignedData<RawKvItem>> output = splitter.addOutput();
							splitter.addOutput().set(ChannelConsumer.ofPromise((r.getStorage().upload())));
							splitter.getInput().set(itemSupplier);
							return output.getSupplier();
						}),
				() -> Promise.of(ChannelSupplier.of()));
	}

	@Override
	public Promise<@Nullable SignedData<RawKvItem>> get(PubKey space, String repo, byte[] key) {
		return simpleRepoMethod(space, repo,
				r -> r.getStorage().get(key),
				(master, r) -> master.get(space, repo, key),
				() -> Promise.of(null));
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		return Promise.of(ensureNamespace(space).getRepos().keySet());
	}

	public Promise<Void> fetch() {
		return Promises.all(namespaces.values().stream().map(GlobalKvNamespace::updateRepositories))
				.then($ -> forEachRepo(Repo::fetch));
	}

	public Promise<Void> fetch(String table) {
		return Promises.all(namespaces.keySet().stream()
				.map(space -> fetch(space, table)));
	}

	public Promise<Void> fetch(PubKey space, String table) {
		return ensureNamespace(space).ensureRepository(table).then(Repo::fetch);
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
		return tolerantCollectVoid(new HashSet<>(namespaces.values()).stream().flatMap(entry -> entry.getRepos().values().stream()), fn);
	}

	@Override
	public String toString() {
		return "GlobalKvNodeImpl{id=" + id + '}';
	}
}
