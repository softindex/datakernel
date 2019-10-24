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

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelOutput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.util.Initializable;
import io.datakernel.util.ref.RefBoolean;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.RawKvItem;
import io.global.kv.api.StorageFactory;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.global.util.Utils.nSuccessesOrLess;

public final class GlobalKvNodeImpl extends AbstractGlobalNode<GlobalKvNodeImpl, GlobalKvNamespace, GlobalKvNode> implements GlobalKvNode, Initializable<GlobalKvNodeImpl> {
	private int uploadCallNumber = 1;
	private int uploadSuccessNumber = 0;

	private boolean doesDownloadCaching = true;
	private boolean doesUploadCaching = false;

	private final StorageFactory storageFactory;

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
	// endregion

	@Override
	protected GlobalKvNamespace createNamespace(PubKey space) {
		return new GlobalKvNamespace(this, space);
	}

	public StorageFactory getStorageFactory() {
		return storageFactory;
	}

	@Override
	public Promise<ChannelConsumer<SignedData<RawKvItem>>> upload(PubKey space, String table) {
		GlobalKvNamespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.then(masters -> ns.ensureRepository(table)
						.then(repo -> {
							if (isMasterFor(space)) {
								return repo.upload();
							}
							return nSuccessesOrLess(uploadCallNumber, masters.stream()
									.map(master -> AsyncSupplier.cast(() -> master.upload(space, table))))
									.map(consumers -> {
										ChannelZeroBuffer<SignedData<RawKvItem>> buffer = new ChannelZeroBuffer<>();

										ChannelSplitter<SignedData<RawKvItem>> splitter = ChannelSplitter.create(buffer.getSupplier())
												.lenient();

										RefBoolean localCompleted = new RefBoolean(false);
										if (doesUploadCaching || consumers.isEmpty()) {
											splitter.addOutput().set(ChannelConsumer.ofPromise(repo.upload())
													.withAcknowledgement(ack ->
															ack.whenComplete(($, e) -> {
																if (e == null) {
																	localCompleted.set(true);
																} else {
																	splitter.close(e);
																}
															})));
										} else {
											localCompleted.set(true);
										}

										Promise<Void> process = splitter.splitInto(consumers, uploadSuccessNumber, localCompleted);
										return buffer.getConsumer().withAcknowledgement(ack -> ack.both(process));
									});
						}));
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawKvItem>>> download(PubKey space, String table, long timestamp) {
		GlobalKvNamespace ns = ensureNamespace(space);
		return ns.ensureRepository(table)
				.then(repo -> repo.download(timestamp)
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
																	ChannelSplitter<SignedData<RawKvItem>> splitter = ChannelSplitter.create();
																	ChannelOutput<SignedData<RawKvItem>> output = splitter.addOutput();
																	splitter.addOutput().set(t.getValue2());
																	splitter.getInput().set(t.getValue1());
																	return output.getSupplier();
																}))));
									});
						}));
	}

	@Override
	public Promise<@Nullable SignedData<RawKvItem>> get(PubKey space, String table, byte[] key) {
		GlobalKvNamespace ns = ensureNamespace(space);
		return ns.ensureRepository(table)
				.then(repo -> simpleMethod(space, master -> master.get(space, table, key)
						.then(item -> {
							if (item != null && doesDownloadCaching) {
								return repo.storage.put(item)
										.map($ -> item);
							}
							return Promise.of(item);
						}), $ -> repo.storage.get(key)));
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		return simpleMethod(space, node -> node.list(space), ns -> Promise.of(ns.getRepoNames()));
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

	private final AsyncSupplier<Void> catchUp = reuse(this::doCatchUp);

	public Promise<Void> catchUp() {
		return catchUp.get();
	}

	private Promise<Void> doCatchUp() {
		return Promises.until($ -> {
			long timestampBegin = now.currentTimeMillis();
			return fetch()
					.map($2 -> now.currentTimeMillis() <= timestampBegin + latencyMargin.toMillis());

		});
	}

	@Override
	public String toString() {
		return "GlobalKvNodeImpl{id=" + id + '}';
	}
}
