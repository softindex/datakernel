package io.global.globalfs.server;

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
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.globalfs.api.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

public class RemoteFsFileSystem implements GlobalFsFileSystem {
	private final GlobalFsNamespace namespace;
	private final FsClient fsClient;
	private final CheckpointStorage checkpointStorage;
	private final GlobalFsName name;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

//	private long updateTimestamp;
//	private final AsyncSupplier<Void> update = reuse(null); // TODO: what is this

	private final AsyncSupplier<Void> catchUp = reuse(() -> Stage.ofCallback(this::catchUpIteration));

	public RemoteFsFileSystem(GlobalFsNamespace namespace, String filesystem, FsClient fsClient, CheckpointStorage checkpointStorage) {
		this.namespace = namespace;
		this.fsClient = fsClient;
		this.checkpointStorage = checkpointStorage;
		this.name = GlobalFsName.of(namespace.getKey(), filesystem);
	}

	// region getters
	public GlobalFsNamespace getNamespace() {
		return namespace;
	}

	@Override
	public GlobalFsName getName() {
		return name;
	}

	public FsClient getFsClient() {
		return fsClient;
	}

	public CheckpointStorage getCheckpointStorage() {
		return checkpointStorage;
	}
	// endregion

	@Override
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

	@Override
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

	@Override
	public Stage<Void> fetch(GlobalFsNode client) {
		checkState(client != namespace.getNode(), "Trying to fetch from itself");

		return client.getFileSystem(name)
				.thenCompose(fs -> fs.list("**"))
				.thenCompose(files ->
						Stages.runSequence(files.stream()
								.map(file -> {
									String fileName = file.getAddress().getFile();
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
												return client.download(file.getAddress(), ourSize, -1) // download missing part or the whole file
														.thenCompose(supplier ->
																upload(fileName, ourSize)
																		.thenCompose(supplier::streamTo));

											});
								})))
				.thenApplyEx(($, e) -> null);
	}

	@Override
	public Stage<SerialConsumer<DataFrame>> upload(String fileName, long offset) {
		return fsClient.upload(fileName, offset)
				.thenApply(consumer -> consumer.apply(new StoringReceiver(name.getPubKey(), fileName, checkpointStorage)));
	}

	@Override
	public Stage<SerialSupplier<DataFrame>> download(String fileName, long offset, long length) {
		return checkpointStorage.getCheckpoints(fileName)
				.thenCompose(checkpoints -> {
					assert Arrays.equals(checkpoints, Arrays.stream(checkpoints).sorted().toArray()) : "Checkpoint array must be sorted!";

					int[] extremes = GlobalFsCheckpoint.getExtremes(checkpoints, offset, length);
					int start = extremes[0];
					int finish = extremes[1];
					return fsClient.download(fileName, checkpoints[start], checkpoints[finish] - checkpoints[start])
							.thenApply(supplier -> supplier.apply(FetcherTransformer.create(fileName, checkpointStorage, checkpoints, start, finish)));
				});
	}

	@Override
	public Stage<List<GlobalFsMetadata>> list(String glob) {
		return fsClient.list(glob).thenApply(res -> res.stream().map(this::into).collect(toList()));
	}

	@Override
	public Stage<Void> delete(String glob) {
		return fsClient.delete(glob);
	}

	@Override
	public Stage<Set<String>> copy(Map<String, String> changes) {
		return fsClient.copy(changes);
	}

	@Override
	public Stage<Set<String>> move(Map<String, String> changes) {
		return fsClient.move(changes);
	}

	public static FileSystemFactory usingSingleClient(FsClient mainClient, String dataFolderName, String checkpointFolderName) {
		return (namespace, fsName) -> {
			String namespaceName = GlobalFsName.serializePubKey(namespace.getKey());

			FsClient data = mainClient.subfolder(dataFolderName);
			FsClient checkpoints = mainClient.subfolder(checkpointFolderName);

			return new RemoteFsFileSystem(namespace, fsName,
					data.subfolder(namespaceName).subfolder(fsName),
					new CheckpointStorageFs(checkpoints.subfolder(namespaceName).subfolder(fsName)));
		};
	}

	public static FileSystemFactory usingSingleClient(FsClient mainClient) {
		return usingSingleClient(mainClient, "data", "checkpoints");
	}

	private static class StoringReceiver extends FramesToByteBufsTransformer {
		private final String fileName;
		private final CheckpointStorage checkpointStorage;

		public StoringReceiver(PubKey pubKey, String fileName, CheckpointStorage checkpointStorage) {
			super(pubKey);
			this.fileName = fileName;
			this.checkpointStorage = checkpointStorage;
		}

		@Override
		protected Stage<Void> receiveCheckpoint(SignedData<GlobalFsCheckpoint> checkpoint) {
			return checkpointStorage.saveCheckpoint(fileName, checkpoint);
		}
	}
}
