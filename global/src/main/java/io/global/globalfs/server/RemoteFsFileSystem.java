package io.global.globalfs.server;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.SignedData;
import io.global.globalfs.api.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.Preconditions.checkState;

public class RemoteFsFileSystem implements GlobalFsFileSystem {
	private final PublicKeyFsGroup group;
	private final FsClient fsClient;
	private final CheckpointStorage checkpointStorage;
	private final GlobalFsName name;
	private Settings settings;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

//	private long updateTimestamp;
//	private final AsyncSupplier<Void> update = reuse(null); // TODO: what is this

	private final AsyncSupplier<Void> catchUp = reuse(() -> Stage.ofCallback(this::catchUpIteration));

	public RemoteFsFileSystem(PublicKeyFsGroup group, GlobalFsName name, FsClient fsClient, CheckpointStorage checkpointStorage) {
		this.group = group;
		this.fsClient = fsClient;
		this.checkpointStorage = checkpointStorage;
		this.name = name;
	}

	// region getters
	public PublicKeyFsGroup getGroup() {
		return group;
	}

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
					if (timestampEnd - started > settings.getLatencyMargin().toMillis()) {
						callback.set(null);
					} else {
						catchUpIteration(callback);
					}
				})
				.whenException(callback::setException);
	}

	@Override
	public Stage<Void> fetch() {
		return group.getServers()
				.thenCompose(servers ->
						Stages.firstSuccessful(servers.stream().map(this::fetch)));
	}

	@Override
	public Stage<Void> fetch(GlobalFsClient client) {
		checkState(client != group.getServer(), "Trying to fetch from itself");

		return client.list(name, "**")
				.thenCompose(files ->
						Stages.runSequence(files.stream()
								.map(file -> {
									return fsClient.getMetadata(file.getName())
											.thenCompose(ourFile -> { // ourFile can be null, but file - can't
												// skip if our file is better
												if (FileMetadata.getMoreCompleteFile(file, ourFile) == ourFile) {
													return Stage.of(null);
												}
												long ourSize = ourFile != null ? ourFile.getSize() : 0;
												return client.download(name, file.getName(), ourSize, -1)
														.thenCompose(producer ->
																upload(file.getName(), ourSize)
																		.thenCompose(producer::streamTo));

											});
								})));
	}

	@Override
	public Stage<SerialConsumer<DataFrame>> upload(String fileName, long offset) {
		return fsClient.upload(fileName, offset)
				.thenApply(consumer ->
						consumer.apply(new FramesToByteBufsTransformer(name.getPubKey()) {
							@Override
							protected Stage<Void> receiveCheckpoint(SignedData<GlobalFsCheckpoint> checkpoint) {
								return checkpointStorage.saveCheckpoint(fileName, checkpoint);
							}
						}));
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
							.thenApply(producer -> producer.apply(FetcherTransformer.create(fileName, checkpointStorage, checkpoints, start, finish)));
				});
	}

	@Override
	public Stage<Revision> list(long revisionId) {
		throw new UnsupportedOperationException("RemoteFsFileSystem#list(long) is not implemented yet");
	}

	@Override
	public Stage<List<FileMetadata>> list(String glob) {
		return fsClient.list(glob);
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

	public interface Settings {
		Duration getLatencyMargin();
	}
}
