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

import io.datakernel.async.*;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.Initializable;
import io.datakernel.util.ref.RefBoolean;
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

import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.util.LogUtils.Level.FINEST;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.util.Utils.nSuccessesOrLess;
import static io.global.util.Utils.tolerantCollectBoolean;

public final class GlobalFsNodeImpl extends AbstractGlobalNode<GlobalFsNodeImpl, GlobalFsNamespace, GlobalFsNode> implements GlobalFsNode, Initializable<GlobalFsNodeImpl> {
	private static final Logger logger = Logger.getLogger(GlobalFsNodeImpl.class.getName());

	private int uploadCallNumber = 1;
	private int uploadSuccessNumber = 0;

	private boolean doesDownloadCaching = true;
	private boolean doesUploadCaching = true;

	private final Function<PubKey, FsClient> storageFactory;
	private final Function<PubKey, CheckpointStorage> checkpointStorageFactory;

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

	public GlobalFsNodeImpl withDownloadCaching(boolean caching) {
		doesDownloadCaching = caching;
		return this;
	}

	public GlobalFsNodeImpl withUploadCaching(boolean caching) {
		doesUploadCaching = caching;
		return this;
	}

	public GlobalFsNodeImpl withoutCaching() {
		return withDownloadCaching(false);
	}

	public GlobalFsNodeImpl withUploadRedundancy(int minUploads, int maxUploads) {
		uploadSuccessNumber = minUploads;
		uploadCallNumber = maxUploads;
		return this;
	}
	// endregion

	@Override
	protected GlobalFsNamespace createNamespace(PubKey space) {
		return new GlobalFsNamespace(this, space);
	}

	public Function<PubKey, FsClient> getStorageFactory() {
		return storageFactory;
	}

	public Function<PubKey, CheckpointStorage> getCheckpointStorageFactory() {
		return checkpointStorageFactory;
	}

	@Override
	public Promise<ChannelConsumer<DataFrame>> upload(PubKey space, String filename, long offset, long revision) {
		GlobalFsNamespace ns = ensureNamespace(space);
		// check only after ensureMasterNodes because it could've made us master
		return ns.ensureMasterNodes()
				.then(masters -> {
					if (isMasterFor(space)) { // check only after ensureMasterNodes because it could've made us master
						return ns.upload(filename, offset, revision);
					}
					return nSuccessesOrLess(uploadCallNumber, masters.stream()
							.map(master -> AsyncSupplier.cast(() -> master.upload(space, filename, offset, revision))))
							.map(consumers -> {
								ChannelZeroBuffer<DataFrame> buffer = new ChannelZeroBuffer<>();
								ChannelSplitter<DataFrame> splitter = ChannelSplitter.create(buffer.getSupplier()).lenient();

								RefBoolean localCompleted = new RefBoolean(false);
								if (doesUploadCaching || consumers.isEmpty()) {
									splitter.addOutput()
											.set(ChannelConsumer.ofPromise(ns.upload(filename, offset, revision))
													.withAcknowledgement(ack -> ack
															.whenComplete((Callback<? super Void>) ($, e) -> {
																if (e == null) {
																	localCompleted.set(true);
																} else {
																	splitter.close(e);
																}
															})));
								} else {
									localCompleted.set(true);
								}

								MaterializedPromise<Void> process = splitter.splitInto(consumers, uploadSuccessNumber, localCompleted);
								return buffer.getConsumer().withAcknowledgement(ack -> ack.both(process));
							});
				})
				.whenComplete(toLogger(logger, "upload", space, filename, offset, this));
	}

	@Override
	public Promise<ChannelSupplier<DataFrame>> download(PubKey space, String filename, long offset, long length) {
		GlobalFsNamespace ns = ensureNamespace(space);
		// if we have cached file and it is same as or better than remote
		return Promises.toTuple(ns.getMetadata(filename), getMetadata(space, filename))
				.then(t -> {
					GlobalFsCheckpoint localMeta = t.getValue1() != null ? t.getValue1().getValue() : null;
					GlobalFsCheckpoint remoteMeta = t.getValue2() != null ? t.getValue2().getValue() : null;

					// if we have cached file and it is same as or better than remote
					if (localMeta != null) {
						if (remoteMeta == null || GlobalFsCheckpoint.COMPARATOR.compare(localMeta, remoteMeta) >= 0) {
							return localMeta.isTombstone() ?
									Promise.ofException(FILE_NOT_FOUND) :
									ns.download(filename, offset, length);
						}
					}
					if (remoteMeta == null || remoteMeta.isTombstone()) {
						return Promise.ofException(FILE_NOT_FOUND);
					}
					return ns.ensureMasterNodes()
							.then(nodes -> Promises.firstSuccessful(nodes.stream()
									.map(node -> node.download(space, filename, offset, length)
											.map(supplier -> {
												if (!doesDownloadCaching) {
													logger.log(Level.FINEST, () -> "Trying to download file at " + filename + " from " + node + "...");
													return supplier;
												}
												logger.log(Level.FINEST, () -> "Trying to download and cache file at " + filename + " from " + node + "...");

												ChannelSplitter<DataFrame> splitter = ChannelSplitter.create(supplier);

												splitter.addOutput().set(ns.upload(filename, offset, remoteMeta.getRevision()));

												return splitter.addOutput()
														.getSupplier()
														.withEndOfStream(eos -> eos.both(splitter.getProcessCompletion()));
											}))
									.iterator()));
				})
				.whenComplete(toLogger(logger, "download", space, filename, offset, length, this));
	}

	@Override
	public Promise<List<SignedData<GlobalFsCheckpoint>>> listEntities(PubKey space, String glob) {
		return simpleMethod(space, node -> node.listEntities(space, glob), ns -> ns.list(glob))
				.whenComplete(toLogger(logger, FINEST, "list", space, glob, this));
	}

	@Override
	public Promise<@Nullable SignedData<GlobalFsCheckpoint>> getMetadata(PubKey space, String filename) {
		return simpleMethod(space, node -> node.getMetadata(space, filename), ns -> ns.getMetadata(filename))
				.mapEx((res, e) -> e != null ? null : res)
				.whenComplete(toLogger(logger, FINEST, "getMetadata", space, filename, this));
	}

	@Override
	public Promise<Void> delete(PubKey space, SignedData<GlobalFsCheckpoint> tombstone) {
		return simpleMethod(space, node -> node.delete(space, tombstone), ns -> ns.delete(tombstone))
				.whenComplete(toLogger(logger, "delete", space, tombstone, this));
	}

	public Promise<Boolean> push() {
		return tolerantCollectBoolean(namespaces.values(), this::push)
				.whenComplete(toLogger(logger, "push", this));
	}

	public Promise<Boolean> push(PubKey space) {
		return push(ensureNamespace(space));
	}

	private Promise<Boolean> push(GlobalFsNamespace ns) {
		return ns.ensureMasterNodes()
				.then(nodes -> tolerantCollectBoolean(nodes, node -> ns.push(node, "**")))
				.whenComplete(toLogger(logger, "push", ns.getSpace(), this));
	}

	public Promise<Boolean> fetch() {
		return tolerantCollectBoolean(getManagedPublicKeys(), this::fetch)
				.whenComplete(toLogger(logger, "fetch", this));
	}

	public Promise<Boolean> fetch(PubKey space) {
		GlobalFsNamespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.then(nodes -> tolerantCollectBoolean(nodes, node -> ns.fetch(node, "**")))
				.whenComplete(toLogger(logger, "fetch", space, this));
	}

	private final AsyncSupplier<Void> catchUp = reuse(this::doCatchUp);

	public Promise<Void> catchUp() {
		return catchUp.get()
				.whenComplete(toLogger(logger, "catchUp", this));
	}

	private Promise<Void> doCatchUp() {
		return Promises.until(
				$1 -> {
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
