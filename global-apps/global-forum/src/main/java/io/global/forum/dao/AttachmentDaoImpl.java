package io.global.forum.dao;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.remotefs.FsClient;

import java.util.Objects;

import static io.global.forum.Utils.generateBase62;

public final class AttachmentDaoImpl implements AttachmentDao {
	private final FsClient fsClient;

	public AttachmentDaoImpl(FsClient fsClient) {
		this.fsClient = fsClient;
	}

	@Override
	public Promise<String> generateGlobalFsId() {
		return Promises.until(() -> Promise.of(generateBase62(GLOBAL_FS_ID_LENGTH)),
				globalFsId -> fsClient.getMetadata(globalFsId)
						.map(Objects::isNull));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> uploadAttachment(String globalFsId) {
		return fsClient.upload(globalFsId);
	}

	@Override
	public Promise<Void> deleteAttachment(String globalFsId) {
		return fsClient.delete(globalFsId);
	}

	@Override
	public Promise<Long> attachmentSize(String globalFsId) {
		return fsClient.getMetadata(globalFsId)
				.then(metadata -> {
					if (metadata == null) {
						return Promise.ofException(ATTACHMENT_NOT_FOUND);
					}
					return Promise.of(metadata.getSize());
				});
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> loadAttachment(String globalFsId, long offset, long limit) {
		return fsClient.download(globalFsId, offset, limit)
				.thenEx((value, e) -> {
					if (e == FsClient.FILE_NOT_FOUND) {
						return Promise.ofException(ATTACHMENT_NOT_FOUND);
					}
					return Promise.of(value, e);
				});
	}
}
