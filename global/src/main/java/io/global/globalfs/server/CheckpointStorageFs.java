package io.global.globalfs.server;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialSupplier;
import io.global.common.SignedData;
import io.global.globalfs.api.GlobalFsCheckpoint;
import io.global.globalsync.util.SerializationUtils;

import java.io.IOException;
import java.util.Arrays;

public class CheckpointStorageFs implements CheckpointStorage {
	private final FsClient fsClient;

	public CheckpointStorageFs(FsClient fsClient) {
		this.fsClient = fsClient;
	}

	@Override
	public Stage<long[]> getCheckpoints(String filename) {
		return fsClient.download(filename)
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.thenCompose(buf -> {
					long[] array = new long[32];
					int size = 0;
					while (buf.canRead()) {
						byte[] bytes = SerializationUtils.readBytes(buf);
						System.out.println("read: " + Arrays.toString(bytes));
						try {
							SignedData<GlobalFsCheckpoint> checkpoint = SignedData.ofBytes(bytes, GlobalFsCheckpoint::ofBytes);
							if (array.length == size) {
								array = Arrays.copyOf(array, size * 2);
							}
							array[size++] = checkpoint.getData().getPosition();
						} catch (IOException e) {
							return Stage.ofException(e);
						}
					}
					return Stage.of(Arrays.stream(array).limit(size).sorted().toArray());
				});
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stage<SignedData<GlobalFsCheckpoint>> loadCheckpoint(String filename, long position) {
		return fsClient.download(filename)
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.thenCompose(buf -> {
					while (buf.canRead()) {
						byte[] bytes = SerializationUtils.readBytes(buf);
						try {
							SignedData<GlobalFsCheckpoint> checkpoint = SignedData.ofBytes(bytes, GlobalFsCheckpoint::ofBytes);
							if (checkpoint.getData().getPosition() == position) {
								return Stage.of(checkpoint);
							}
						} catch (IOException e) {
							return Stage.ofException(e);
						}
					}
					return Stage.of(null);
				});
	}

	@Override
	public Stage<Void> saveCheckpoint(String filename, SignedData<GlobalFsCheckpoint> checkpoint) {
		return fsClient.getMetadata(filename)
				.thenCompose(m -> fsClient.upload(filename, m != null ? m.getSize() : 0))
				.thenCompose(consumer -> {
					byte[] bytes = checkpoint.toBytes();
					ByteBuf buf = ByteBufPool.allocate(bytes.length + 5);
					SerializationUtils.writeBytes(buf, bytes);
					return SerialSupplier.of(buf).streamTo(consumer);
				});
	}
}
