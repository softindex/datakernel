/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.common.Initializable;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelOutput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.RetryPolicy;
import io.datakernel.remotefs.FsClient;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.CheckpointStorage;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsNode;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;

import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.datakernel.async.util.LogUtils.Level.TRACE;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.global.util.Utils.tolerantCollectBoolean;
import static io.global.util.Utils.untilTrue;

public final class GlobalFsNodeImpl extends AbstractGlobalNode<GlobalFsNodeImpl, GlobalFsNamespace, GlobalFsNode> implements GlobalFsNode, Initializable<GlobalFsNodeImpl> {
	private static final Logger logger = LoggerFactory.getLogger(GlobalFsNodeImpl.class);

	public static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicy.immediateRetry().withMaxTotalRetryCount(10);

	private final Function<PubKey, FsClient> storageFactory;
	private final Function<PubKey, CheckpointStorage> checkpointStorageFactory;
	RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;

	// region creators
	private GlobalFsNodeImpl(RawServerId id, DiscoveryService discoveryService,
			Function<RawServerId, GlobalFsNode> nodeFactory,
			Function<PubKey, FsClient> storageFactory,
			Function<PubKey, CheckpointStorage> checkpointStorageFactory) {
		super(id, discoveryService, nodeFactory);
		this.storageFactory = storageFactory;
		this.checkpointStorageFactory = checkpointStorageFactory;
	}

	public static GlobalFsNodeImpl create(RawServerId id, DiscoveryService discoveryService,
			Function<RawServerId, GlobalFsNode> nodeFactory,
			Function<PubKey, FsClient> storageFactory,
			Function<PubKey, CheckpointStorage> checkpointStorageFactory) {
		return new GlobalFsNodeImpl(id, discoveryService, nodeFactory, storageFactory, checkpointStorageFactory);
	}

	public static GlobalFsNodeImpl create(RawServerId id, DiscoveryService discoveryService, Function<RawServerId, GlobalFsNode> nodeFactory, FsClient fsClient) {
		FsClient data = fsClient.subfolder("data");
		FsClient checkpoints = fsClient.subfolder("checkpoints");
		return new GlobalFsNodeImpl(id, discoveryService, nodeFactory,
				key -> data.subfolder(key.asString()),
				key -> new RemoteFsCheckpointStorage(checkpoints.subfolder(key.asString())));
	}

	public GlobalFsNodeImpl withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}
	// endregion

	@Override
	protected GlobalFsNamespace createNamespace(PubKey space) {
		GlobalFsNamespace ns = new GlobalFsNamespace(this, space);
		ns.fetch();
		return ns;
	}

	public Function<PubKey, FsClient> getStorageFactory() {
		return storageFactory;
	}

	public Function<PubKey, CheckpointStorage> getCheckpointStorageFactory() {
		return checkpointStorageFactory;
	}

	public RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}

	@Override
	public Promise<ChannelConsumer<DataFrame>> upload(PubKey space, String filename, long offset, long revision) {
		GlobalFsNamespace ns = ensureNamespace(space);
		// check only after ensureMasterNodes because it could've made us master
		return ns.upload(filename, offset, revision)
				.map(consumer -> consumer.withAcknowledgement(ack -> ack
						.whenResult($ -> {
							if (!isMasterFor(space)) {
								ns.push(filename);
							}
						})))
				.whenComplete(toLogger(logger, "upload", space, filename, offset, this));
	}

	@Override
	public Promise<ChannelSupplier<DataFrame>> download(PubKey space, String filename, long offset, long length) {
		GlobalFsNamespace ns = ensureNamespace(space);
		return ns.getMetadata(filename)
				.then(metadata -> {
					if (metadata != null) {
						return ns.download(filename, offset, length);
					}
					return simpleMethod(space,
							master -> master.getMetadata(space, filename)
									.then(meta -> meta == null ?
											Promise.ofException(FILE_NOT_FOUND) :
											master.download(space, filename, offset, length)
													.map(supplier -> {
														ChannelSplitter<DataFrame> splitter = ChannelSplitter.create();
														ChannelOutput<DataFrame> output = splitter.addOutput();
														splitter.addOutput().set(ChannelConsumer.ofPromise(ns.upload(filename, offset, meta.getValue().getRevision())));
														splitter.getInput().set(supplier);
														return output.getSupplier();
													})),
							$ -> Promise.ofException(FILE_NOT_FOUND));
				})
				.whenComplete(toLogger(logger, "download", space, filename, offset, length, this));
	}

	@Override
	public Promise<List<SignedData<GlobalFsCheckpoint>>> listEntities(PubKey space, String glob) {
		return ensureNamespace(space).list(glob)
				.whenComplete(toLogger(logger, TRACE, "list", space, glob, this));
	}

	@Override
	public Promise<@Nullable SignedData<GlobalFsCheckpoint>> getMetadata(PubKey space, String filename) {
		GlobalFsNamespace ns = ensureNamespace(space);
		return ns.getMetadata(filename)
				.thenEx((signedData, e) -> {
					if (!isMasterFor(space) && (signedData == null || e != null)) {
						return ns.ensureMasterNodes()
								.then(masters -> Promises.firstSuccessful(masters.stream()
										.map(master -> AsyncSupplier.cast(() -> master.getMetadata(space, filename)))));
					}
					return Promise.of(signedData);
				})
				.mapEx((res, e) -> e != null ? null : res)
				.whenComplete(toLogger(logger, TRACE, "getMetadata", space, filename, this));
	}

	@Override
	public Promise<Void> delete(PubKey space, SignedData<GlobalFsCheckpoint> tombstone) {
		GlobalFsNamespace ns = ensureNamespace(space);
		return ns.delete(tombstone)
				.whenResult($ -> ns.push(tombstone.getValue().getFilename()))
				.whenComplete(toLogger(logger, "delete", space, tombstone, this));
	}

	public Promise<Boolean> push() {
		return tolerantCollectBoolean(namespaces.values(), GlobalFsNamespace::push)
				.whenComplete(toLogger(logger, TRACE, "push", this));
	}

	public Promise<Boolean> fetch() {
		return tolerantCollectBoolean(namespaces.values(), GlobalFsNamespace::fetch)
				.whenComplete(toLogger(logger, TRACE, "fetch", this));
	}

	public Promise<Boolean> fetch(PubKey space, String glob) {
		GlobalFsNamespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.then(masters -> {
					if (masters.isEmpty()) {
						return Promise.of(false);
					}
					return Promises.firstSuccessful(masters.stream()
							.map(master -> AsyncSupplier.cast(() -> ns.fetch(master, glob))));
				});
	}

	private final AsyncSupplier<Void> catchUp = reuse(this::doCatchUp);

	public Promise<Void> catchUp() {
		return catchUp.get()
				.whenComplete(toLogger(logger, TRACE, "catchUp", this));
	}

	private Promise<Void> doCatchUp() {
		return untilTrue(() -> {
			long timestampBegin = now.currentTimeMillis();
			return fetch()
					.map(didAnything ->
							!didAnything || now.currentTimeMillis() <= timestampBegin + latencyMargin.toMillis());
		});
	}

	@Override
	public String toString() {
		return "GlobalFsNodeImpl{id=" + id + '}';
	}
}
