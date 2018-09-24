package io.global.globalfs.client;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import io.global.globalfs.api.CheckpointPositionStrategy;
import io.global.globalfs.api.FramesToByteBufsTransformer;
import io.global.globalfs.api.GlobalFsFileSystem;
import io.global.globalfs.api.SignerTransformer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;

public class GlobalFsAdapter implements FsClient {
	private final GlobalFsFileSystem fs;
	private final KeyPair keys;
	private final CheckpointPositionStrategy checkpointPositionStrategy;

	public GlobalFsAdapter(GlobalFsFileSystem fs, KeyPair keys, CheckpointPositionStrategy checkpointPositionStrategy) {
		this.fs = fs;
		this.keys = keys;
		this.checkpointPositionStrategy = checkpointPositionStrategy;
	}

	@Override
	public Stage<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		return fs.upload(filename, offset == -1 ? 0 : offset)
				.thenApply(consumer -> consumer
						.apply(SignerTransformer.create(offset == -1 ? 0 : offset, checkpointPositionStrategy, keys.getPrivKey())));
	}

	@Override
	public Stage<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return fs.download(filename, offset, length)
				.thenApply(supplier -> supplier.apply(new CuttingReceiver(keys.getPubKey(), offset, length)));
	}

	@Override
	public Stage<Set<String>> move(Map<String, String> changes) {
		return fs.move(changes);
	}

	@Override
	public Stage<Set<String>> copy(Map<String, String> changes) {
		return fs.copy(changes);
	}

	@Override
	public Stage<List<FileMetadata>> list(String glob) {
		return fs.list(glob)
				.thenApply(res -> res.stream()
						.map(meta -> new FileMetadata(meta.getAddress().getFile(), meta.getSize(), meta.getRevision()))
						.collect(toList()));
	}

	@Override
	public Stage<Void> delete(String glob) {
		return fs.delete(glob);
	}

	private static class CuttingReceiver extends FramesToByteBufsTransformer {
		private final long offset;
		private final long endOffset;

		CuttingReceiver(PubKey pubKey, long offset, long length) {
			super(pubKey);
			this.offset = offset;
			this.endOffset = length == -1 ? Long.MAX_VALUE : offset + length;
		}

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
	}
}
