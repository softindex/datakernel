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

package io.datakernel.remotefs;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.functional.Try;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.StageStats;
import io.datakernel.serial.processor.SerialSplitter;
import io.datakernel.util.Initializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static io.datakernel.file.FileUtils.isWildcard;
import static io.datakernel.serial.SerialConsumer.getAcknowledgement;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

public final class RemoteFsRepartitionController implements Initializable<RemoteFsRepartitionController>, EventloopJmxMBeanEx, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsRepartitionController.class);

	private final Eventloop eventloop;
	private final Object localPartitionId;
	private final RemoteFsClusterClient cluster;
	private final FsClient localStorage;
	private final ServerSelector serverSelector;
	private final Map<Object, FsClient> clients;
	private final int replicationCount;

	private String glob = "**";
	private String negativeGlob = "";

	private int allFiles = 0;
	private int ensuredFiles = 0;
	private int failedFiles = 0;

	@Nullable
	private Stage<Void> repartitionStage;

	private final StageStats repartitionStageStats = StageStats.create(Duration.ofMinutes(5));
	private final StageStats singleFileRepartitionStageStats = StageStats.create(Duration.ofMinutes(5));

	private RemoteFsRepartitionController(Eventloop eventloop, Object localPartitionId, RemoteFsClusterClient cluster,
			FsClient localStorage, ServerSelector serverSelector, Map<Object, FsClient> clients,
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
		return new RemoteFsRepartitionController(cluster.getEventloop(), localPartitionId, cluster, cluster.getClients().get(localPartitionId),
				cluster.getServerSelector(), cluster.getAliveClients(), cluster.getReplicationCount());
	}

	public RemoteFsRepartitionController withGlob(String glob) {
		this.glob = checkNotNull(glob, "glob");
		return this;
	}

	public RemoteFsRepartitionController withNegativeGlob(String negativeGlob) {
		this.negativeGlob = checkNotNull(negativeGlob, "negativeGlob");
		return this;
	}

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

	public Stage<Void> repartition() {
		checkState(eventloop.inEventloopThread(), "Should be called from eventloop thread");

		repartitionStage = localStorage.list(glob)
				.thenCompose(list -> {
					allFiles = list.size();
					return Stages.runSequence( // just handling all local files sequentially
							filterNot(list.stream(), negativeGlob)
									.map(meta -> repartitionFile(meta)
											.whenComplete(singleFileRepartitionStageStats.recordStats())
											.thenCompose(success -> {
												if (success) {
													ensuredFiles++;
												} else {
													failedFiles++;
												}
												if (repartitionStage == null) {
													return Stage.ofException(new Exception("forced stop"));
												}
												return Stage.complete();
											})));
				})
				.whenComplete(repartitionStageStats.recordStats())
				.thenComposeEx(($, err) -> {
					if (err != null) {
						logger.warn("forced repartition finish, {} files ensured, {} errored, {} untouched", ensuredFiles, failedFiles, allFiles - ensuredFiles - failedFiles);
					} else {
						logger.info("repartition finished, {} files ensured, {} errored", ensuredFiles, failedFiles);
					}
					repartitionStage = null;
					return Stage.complete();
				});
		return repartitionStage;
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

	private Stage<Boolean> repartitionFile(FileMetadata meta) {
		Set<Object> partitionIds = new HashSet<>(clients.keySet());
		partitionIds.add(localPartitionId); // ensure local partition could also be selected
		List<Object> selected = serverSelector.selectFrom(meta.getName(), partitionIds, replicationCount);

		return getPartitionsWithoutFile(meta, selected)
				.thenCompose(uploadTargets -> {
					if (uploadTargets == null) { // null return means failure
						return Stage.of(false);
					}
					String name = meta.getName();
					if (uploadTargets.isEmpty()) { // everybody had the file
						logger.trace("deleting file {} locally", meta);
						return localStorage.delete(name) // so we delete the copy which does not belong to local partition
								.thenApply($ -> {
									logger.info("handled file {} (ensured on {})", meta, selected);
									return true;
								});
					}
					if (uploadTargets.size() == 1 && uploadTargets.get(0) == localStorage) { // everybody had the file AND
						logger.info("handled file {} (ensured on {})", meta, selected);      // we dont delete the local copy
						return Stage.of(true);
					}

					// else we need to upload to at least one nonlocal partition

					logger.trace("uploading file {} to partitions {}...", meta, uploadTargets);

					SerialSplitter<ByteBuf> splitter = SerialSplitter.<ByteBuf>create()
							.withInput(localStorage.downloadSerial(name));

					// recycle original non-slice buffer
					return Stages.toList(uploadTargets.stream() // upload file to target partitions
							.map(partitionId -> {
								if (partitionId == localPartitionId) {
									return Stage.of(Try.of(null)); // just skip it here
								}
								// upload file to this partition
								return getAcknowledgement(cb ->
										splitter.addOutput()
												.set(clients.get(partitionId) // upload file to this partition
														.uploadSerial(name)
														.withAcknowledgement(cb)))
										.whenException(e -> {
											logger.warn("failed uploading to partition " + partitionId + " (" + e + ')');
											cluster.markDead(partitionId, e);
										})
										.whenResult($ -> logger.trace("file {} uploaded to '{}'", meta, partitionId))
										.toTry();
							}))
							.thenCompose(tries -> {
								if (!tries.stream().allMatch(Try::isSuccess)) { // if anybody failed uploading then we skip this file
									logger.warn("failed uploading file {}, skipping", meta);
									return Stage.of(false);
								}

								if (uploadTargets.contains(localPartitionId)) { // dont delete local if it was marked
									logger.info("handled file {} (ensured on {}, uploaded to {})", meta, selected, uploadTargets);
									return Stage.of(true);
								}

								logger.trace("deleting file {} on {}", meta, localPartitionId);
								return localStorage.delete(name)
										.thenApply($ -> {
											logger.info("handled file {} (ensured on {}, uploaded to {})", meta, selected, uploadTargets);
											return true;
										});
							});
				})
				.whenComplete(toLogger(logger, TRACE, "repartitionFile", meta));
	}

	private Stage<List<Object>> getPartitionsWithoutFile(FileMetadata fileToUpload, List<Object> selected) {
		List<Object> uploadTargets = new ArrayList<>();
		return Stages.toList(selected.stream()
				.map(partitionId -> {
					if (partitionId == localPartitionId) {
						uploadTargets.add(partitionId); // add it to targets so in repartitionFile we know not to delete local file
						return Stage.of(Try.of(null));  // and skip other logic
					}
					return clients.get(partitionId)
							.list(fileToUpload.getName()) // checking file existense and size on particular partition
							.whenComplete((list, err) -> {
								if (err != null) {
									logger.warn("failed connecting to partition " + partitionId + " (" + err + ')');
									cluster.markDead(partitionId, err);
									return;
								}
								// ↓ when there is no file or it is smaller than ours
								if (list.isEmpty() || list.get(0).getSize() < fileToUpload.getSize()) {
									uploadTargets.add(partitionId);
								}
							})
							.toTry();
				}))
				.thenCompose(tries -> {
					if (!tries.stream().allMatch(Try::isSuccess)) { // any of list calls failed
						logger.warn("failed figuring out partitions for file " + fileToUpload + ", skipping");
						return Stage.of(null); // using null to mark failure without exceptions
					}
					return Stage.of(uploadTargets);
				});
	}

	@Override
	public Stage<Void> start() {
		return Stage.complete();
	}

	@Override
	public Stage<Void> stop() {
		Stage<Void> repartitionStage = this.repartitionStage;
		if (repartitionStage != null) {
			this.repartitionStage = null;
			return repartitionStage;
		}
		return Stage.complete();
	}

	// region JMX
	@JmxOperation(description = "start repartitioning")
	public void startRepartition() {
		repartition();
	}

	@JmxOperation(description = "stop repartitioning")
	public void stopRepartition() {
		repartitionStage = null;
	}

	@JmxAttribute
	public boolean isRepartitioning() {
		return repartitionStage != null;
	}

	@JmxAttribute
	public StageStats getRepartitionStageStats() {
		return repartitionStageStats;
	}

	@JmxAttribute
	public StageStats getSingleFileRepartitionStageStats() {
		return singleFileRepartitionStageStats;
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
