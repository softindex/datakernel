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
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.time.CurrentTimeProvider;
import io.global.globalfs.api.*;
import io.global.globalfs.local.LocalGlobalFsNode.FileSystemFactory;
import io.global.globalfs.transformers.FramesFromStorage;
import io.global.globalfs.transformers.FramesIntoStorage;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

final class RemoteFsFileSystem {
	private final LocalGlobalFsNamespace namespace;
	private final FsClient fsClient;
	private final CheckpointStorage checkpointStorage;
	private final GlobalFsName name;
	private final AsyncSupplier<Void> catchUp = reuse(() -> Stage.ofCallback(this::catchUpIteration));
	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	// region creators
	public RemoteFsFileSystem(LocalGlobalFsNamespace namespace, String filesystem, FsClient fsClient, CheckpointStorage checkpointStorage) {
		this.namespace = namespace;
		this.fsClient = fsClient;
		this.checkpointStorage = checkpointStorage;
		this.name = GlobalFsName.of(namespace.getKey(), filesystem);
	}
	// endregion

	public static FileSystemFactory usingSingleClient(FsClient mainClient, String dataFolderName, String checkpointFolderName) {
		return (namespace, fsName) -> {
			String namespaceName = GlobalFsName.serializePubKey(namespace.getKey());

			FsClient data = mainClient.subfolder(dataFolderName);
			FsClient checkpoints = mainClient.subfolder(checkpointFolderName);

			return new RemoteFsFileSystem(namespace, fsName,
					data.subfolder(namespaceName).subfolder(fsName),
					new RemoteFsCheckpointStorage(checkpoints.subfolder(namespaceName).subfolder(fsName)));
		};
	}

	public static FileSystemFactory usingSingleClient(FsClient mainClient) {
		return usingSingleClient(mainClient, "data", "checkpoints");
	}

	public Stage<Void> catchUp() {
		return catchUp.get();
	}

	private void catchUpIteration(SettableStage<Void> callback) {
		long started = now.currentTimeMillis();
		fetch()
				.thenRun(() -> {
					long timestampEnd = now.currentTimeMillis();
					if (timestampEnd - started > namespace.getNode().getSettings().getLatencyMargin().toMillis()) {
						callback.set(null);
					} else {
						catchUpIteration(callback);
					}
				})
				.whenException(callback::setException);
	}

	public Stage<Void> fetch() {
		return namespace.findNodes()
				.thenCompose(servers ->
						Stages.firstSuccessful(servers.stream().map(this::fetch)));
	}

	@Nullable
	private GlobalFsMetadata into(@Nullable FileMetadata meta) {
		if (meta == null) {
			return null;
		}
		return new GlobalFsMetadata(name.addressOf(meta.getName()), meta.getSize(), meta.getTimestamp());
	}

	public Stage<Void> fetch(GlobalFsNode client) {
		checkState(client != namespace.getNode(), "Trying to fetch from itself");

		return client.getFileSystem(name)
				.list("**")
				.thenCompose(files ->
						Stages.runSequence(files.stream()
								.map(file -> {
									String fileName = file.getPath().getPath();
									return fsClient.getMetadata(fileName)
											.thenCompose(ourFileMeta -> {
												GlobalFsMetadata ourFile = into(ourFileMeta);
												// skip if our file is better
												// ourFile can be null, but file - can't,
												// so it will proceed to download a new file
												if (GlobalFsMetadata.getBetter(file, ourFile) == ourFile) {
													return Stage.of(null);
												}
												long ourSize = ourFileMeta != null ? ourFileMeta.getSize() : 0;
												return client.download(file.getPath(), ourSize, -1) // download missing part or the whole file
														.thenCompose(supplier ->
																upload(fileName, ourSize)
																		.thenCompose(supplier::streamTo));

											});
								})))
				.thenApplyEx(($, e) -> null);
	}

	public Stage<SerialConsumer<DataFrame>> upload(String fileName, long offset) {
		return fsClient.upload(fileName, offset)
				.thenApply(consumer -> consumer.apply(new FramesIntoStorage(name.getPubKey(), fileName, checkpointStorage)));
	}

	public Stage<SerialSupplier<DataFrame>> download(String fileName, long offset, long length) {
		return checkpointStorage.getCheckpoints(fileName)
				.thenCompose(checkpoints -> {
					assert Arrays.equals(checkpoints, Arrays.stream(checkpoints).sorted().toArray()) : "Checkpoint array must be sorted!";

					int[] extremes = GlobalFsCheckpoint.getExtremes(checkpoints, offset, length);
					int start = extremes[0];
					int finish = extremes[1];
					return fsClient.download(fileName, checkpoints[start], checkpoints[finish] - checkpoints[start])
							.thenApply(supplier -> supplier.apply(FramesFromStorage.create(fileName, checkpointStorage, checkpoints, start, finish)));
				});
	}

	public Stage<List<GlobalFsMetadata>> list(String glob) {
		return fsClient.list(glob).thenApply(res -> res.stream().map(this::into).collect(toList()));
	}

	public Stage<Void> delete(String glob) {
		return fsClient.delete(glob);
	}

	public Stage<Set<String>> copy(Map<String, String> changes) {
		return fsClient.copy(changes);
	}

	public Stage<Set<String>> move(Map<String, String> changes) {
		return fsClient.move(changes);
	}
}
