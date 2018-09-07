package io.global.globalfs.client;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.global.common.PrivKey;
import io.global.globalfs.api.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GlobalFsAdapter implements FsClient {
	private final GlobalFsClient globalFsClient;
	private final GlobalFsName name;
	private final CheckpointPositionStrategy checkpointPositionStrategy;
	private final PrivKey privateKey;

	public GlobalFsAdapter(GlobalFsClient globalFsClient, GlobalFsName name, CheckpointPositionStrategy checkpointPositionStrategy, PrivKey privateKey) {
		this.globalFsClient = globalFsClient;
		this.name = name;
		this.checkpointPositionStrategy = checkpointPositionStrategy;
		this.privateKey = privateKey;
	}

	@Override
	public Stage<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		return globalFsClient.upload(name, filename, offset)
				.thenApply(consumer -> consumer
						.apply(SignerTransformer.create(offset, checkpointPositionStrategy, privateKey)));
	}

	@Override
	public Stage<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return globalFsClient.download(name, filename, offset, length)
				.thenApply(supplier ->
						supplier.apply(new FramesToByteBufsTransformer(name.getPubKey()) {
							long endOffset = offset + length;

							@Override
							protected Stage<Void> receiveByteBuffer(ByteBuf byteBuf) {
								int size = byteBuf.readRemaining();
								if (position <= offset || position - size > endOffset) {
									return Stage.of(null);
								}
								if (position - size < offset) {
									byteBuf.moveReadPosition((int) (offset - position + size));
								}
								if (position > endOffset) {
									byteBuf.moveWritePosition((int) (endOffset - position));
								}
								return output.accept(byteBuf);
							}
						}));
	}

	@Override
	public Stage<Set<String>> move(Map<String, String> changes) {
		return globalFsClient.move(name, changes);
	}

	@Override
	public Stage<Set<String>> copy(Map<String, String> changes) {
		return globalFsClient.copy(name, changes);
	}

	@Override
	public Stage<List<FileMetadata>> list(String glob) {
		return globalFsClient.list(name, glob);
	}

	@Override
	public Stage<Void> delete(String glob) {
		return globalFsClient.delete(name, glob);
	}
}
