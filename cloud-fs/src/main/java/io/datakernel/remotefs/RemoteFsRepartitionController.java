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

package io.datakernel.remotefs;

import io.datakernel.async.function.AsyncSuppliers;
import io.datakernel.async.function.AsyncSuppliers.AsyncSupplierWithStatus;
import io.datakernel.async.service.EventloopService;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.Initializable;
import io.datakernel.common.collection.Try;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxOperation;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.SettablePromise;
import io.datakernel.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static io.datakernel.async.util.LogUtils.Level.TRACE;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.csp.ChannelConsumer.getAcknowledgement;
import static io.datakernel.remotefs.RemoteFsUtils.isWildcard;

public final class RemoteFsRepartitionController implements Initializable<RemoteFsRepartitionController>, EventloopJmxMBeanEx, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsRepartitionController.class);

	private final Eventloop eventloop;
	private final Object localPartitionId;
	private final RemoteFsClusterClient cluster;
	private final LocalFsClient localStorage;
	private final ServerSelector serverSelector;
	private final Map<Object, FsClient> clients;
	private final int replicationCount;

	private String glob = "**";
	private String negativeGlob = "";

	private int allFiles = 0;
	private int ensuredFiles = 0;
	private int failedFiles = 0;

	@Nullable
	private SettablePromise<Void> closeCallback;

	private final PromiseStats repartitionPromiseStats = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats singleFileRepartitionPromiseStats = PromiseStats.create(Duration.ofMinutes(5));

	private RemoteFsRepartitionController(Eventloop eventloop, Object localPartitionId, RemoteFsClusterClient cluster,
										  LocalFsClient localStorage, ServerSelector serverSelector, Map<Object, FsClient> clients,
										  int replicationCount) {
		this.eventloop = eventloop;
		this.localPartitionId = localPartitionId;
		this.cluster = cluster;
		this.localStorage = localStorage;
		this.serverSelector = serverSelector;
		this.clients = clients;
		this.replicationCount = replicationCount;
	}

	public static RemoteFsRepartitionController create(Object localPartitionId, RemoteFsClusterClient cluster) {
		FsClient local = cluster.getClients().get(localPartitionId);

		checkState(local instanceof LocalFsClient, "Local partition should be actually local and be an instance of LocalFsClient");

		return new RemoteFsRepartitionController(cluster.getEventloop(), localPartitionId, cluster, (LocalFsClient) local,
				cluster.getServerSelector(), cluster.getAliveClients(), cluster.getReplicationCount());
	}

	public RemoteFsRepartitionController withGlob(@NotNull String glob) {
		this.glob = glob;
		return this;
	}

	public RemoteFsRepartitionController withNegativeGlob(@NotNull String negativeGlob) {
		this.negativeGlob = negativeGlob;
		return this;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public Object getLocalPartitionId() {
		return localPartitionId;
	}

	public RemoteFsClusterClient getCluster() {
		return cluster;
	}

	public FsClient getLocalStorage() {
		return localStorage;
	}

	@NotNull
	public Promise<Void> repartition() {
		return repartition.get();
	}

	private final AsyncSupplierWithStatus<Void> repartition = new AsyncSupplierWithStatus<>(AsyncSuppliers.reuse(this::doRepartition));

	@NotNull
	private Promise<Void> doRepartition() {
		checkState(eventloop.inEventloopThread(), "Should be called from eventloop thread");

		return localStorage.list(glob)
				.then(list -> {
					allFiles = list.size();
					return Promises.sequence( // just handling all local files sequentially
							filterNot(list.stream(), negativeGlob)
									.map(meta ->
											() -> repartitionFile(meta)
													.whenComplete(singleFileRepartitionPromiseStats.recordStats())
													.then((Boolean success) -> {
														if (success) {
															ensuredFiles++;
														} else {
															failedFiles++;
														}
														return Promise.complete();
													})));
				})
				.whenComplete(repartitionPromiseStats.recordStats())
				.thenEx(($, e) -> {
					if (e != null) {
						logger.warn("forced repartition finish, {} files ensured, {} errored, {} untouched", ensuredFiles, failedFiles, allFiles - ensuredFiles - failedFiles);
					} else {
						logger.info("repartition finished, {} files ensured, {} errored", ensuredFiles, failedFiles);
					}
					if (closeCallback != null) {
						closeCallback.accept($, e);
					}
					return Promise.complete();
				});
	}

	private Stream<FileMetadata> filterNot(Stream<FileMetadata> stream, String glob) {
		if (glob.isEmpty()) {
			return stream;
		}
		if (!isWildcard(glob)) {
			return stream.filter(file -> !file.getName().equals(negativeGlob));
		}
		PathMatcher negativeMatcher = FileSystems.getDefault().getPathMatcher("glob:" + negativeGlob);
		return stream.filter(file -> !negativeMatcher.matches(Paths.get(file.getName())));
	}

	private Promise<Boolean> repartitionFile(FileMetadata meta) {
		Set<Object> partitionIds = new HashSet<>(clients.keySet());
		partitionIds.add(localPartitionId); // ensure local partition could also be selected
		List<Object> selected = serverSelector.selectFrom(meta.getName(), partitionIds, replicationCount);

		return getPartitionsThatNeedOurFile(meta, selected)
				.then(uploadTargets -> {
					if (uploadTargets == null) { // null return means failure
						return Promise.of(false);
					}
					String name = meta.getName();
					long revision = meta.getRevision();
					if (uploadTargets.isEmpty()) { // everybody had the file
						logger.trace("deleting file {} locally", meta);
						return localStorage.remove(name) // so we delete the copy which does not belong to local partition
								.map($ -> {
									logger.info("handled file {} (ensured on {})", meta, selected);
									return true;
								});
					}
					if (uploadTargets.size() == 1 && uploadTargets.get(0) == localStorage) { // everybody had the file AND
						logger.info("handled file {} (ensured on {})", meta, selected);      // we dont delete the local copy
						return Promise.of(true);
					}

					// else we need to upload to at least one nonlocal partition

					logger.trace("uploading file {} to partitions {}...", meta, uploadTargets);

					ChannelSplitter<ByteBuf> splitter = ChannelSplitter.<ByteBuf>create()
							.withInput(ChannelSupplier.ofPromise(localStorage.download(name)));

					// recycle original non-slice buffer
					return Promises.toList(uploadTargets.stream() // upload file to target partitions
							.map(partitionId -> {
								if (partitionId == localPartitionId) {
									return Promise.of(Try.of(null)); // just skip it here
								}
								// upload file to this partition
								return getAcknowledgement(fn ->
										splitter.addOutput()
												.set(ChannelConsumer.ofPromise(clients.get(partitionId).upload(name, 0, revision))
														.withAcknowledgement(fn)))
										.whenException(e -> {
											logger.warn("failed uploading to partition " + partitionId + " (" + e + ')');
											cluster.markDead(partitionId, e);
										})
										.whenResult($ -> logger.trace("file {} uploaded to '{}'", meta, partitionId))
										.toTry();
							}))
							.then(tries -> {
								if (!tries.stream().allMatch(Try::isSuccess)) { // if anybody failed uploading then we skip this file
									logger.warn("failed uploading file {}, skipping", meta);
									return Promise.of(false);
								}

								if (uploadTargets.contains(localPartitionId)) { // dont delete local if it was marked
									logger.info("handled file {} (ensured on {}, uploaded to {})", meta, selected, uploadTargets);
									return Promise.of(true);
								}

								logger.trace("deleting file {} on {}", meta, localPartitionId);
								return localStorage.remove(name)
										.map($ -> {
											logger.info("handled file {} (ensured on {}, uploaded to {})", meta, selected, uploadTargets);
											return true;
										});
							});
				})
				.whenComplete(toLogger(logger, TRACE, "repartitionFile", meta));
	}

	private Promise<List<Object>> getPartitionsThatNeedOurFile(FileMetadata fileToUpload, List<Object> selected) {
		List<Object> uploadTargets = new ArrayList<>();
		return Promises.toList(selected.stream()
				.map(partitionId -> {
					if (partitionId == localPartitionId) {
						uploadTargets.add(partitionId); // add it to targets so in repartitionFile we know not to delete local file
						return Promise.of(Try.of(null));  // and skip other logic
					}
					return clients.get(partitionId)
							.listEntities(fileToUpload.getName()) // checking file existense and size on particular partition
							.whenComplete((list, e) -> {
								if (e != null) {
									logger.warn("failed connecting to partition " + partitionId + " (" + e + ')');
									cluster.markDead(partitionId, e);
									return;
								}
								// ↓ when there is no file or it is worse than ours
								if (list.isEmpty() || FileMetadata.COMPARATOR.compare(list.get(0), fileToUpload) < 0) {
									uploadTargets.add(partitionId);
								}
							})
							.toTry();
				}))
				.then(tries -> {
					if (!tries.stream().allMatch(Try::isSuccess)) { // any of list calls failed
						logger.warn("failed figuring out partitions for file " + fileToUpload + ", skipping");
						return Promise.of(null); // using null to mark failure without exceptions
					}
					return Promise.of(uploadTargets);
				});
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return repartition.isRunning() ?
				Promise.ofCallback(cb -> this.closeCallback = cb) :
				Promise.complete();
	}

	// region JMX
	@JmxOperation(description = "start repartitioning")
	public void startRepartition() {
		repartition();
	}

	@JmxAttribute
	public boolean isRepartitioning() {
		return repartition.isRunning();
	}

	@JmxAttribute
	public PromiseStats getRepartitionPromiseStats() {
		return repartitionPromiseStats;
	}

	@JmxAttribute
	public PromiseStats getSingleFileRepartitionPromiseStats() {
		return singleFileRepartitionPromiseStats;
	}

	@JmxAttribute
	public int getLastFilesToRepartition() {
		return allFiles;
	}

	@JmxAttribute
	public int getLastEnsuredFiles() {
		return ensuredFiles;
	}

	@JmxAttribute
	public int getLastFailedFiles() {
		return failedFiles;
	}
	// endregion
}
