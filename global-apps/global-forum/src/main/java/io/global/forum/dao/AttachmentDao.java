package io.global.forum.dao;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.StacklessException;
import io.datakernel.util.ApplicationSettings;

import java.util.Set;

public interface AttachmentDao {
	int GLOBAL_FS_ID_LENGTH = ApplicationSettings.getInt(AttachmentDao.class, "globalFsIdLength", 10);
	StacklessException ATTACHMENT_NOT_FOUND = new StacklessException(AttachmentDao.class, "Attachment not found");

	Promise<String> generateGlobalFsId();

	Promise<ChannelConsumer<ByteBuf>> uploadAttachment(String globalFsId);

	default Promise<Void> deleteAttachments(Set<String> globalFsIds) {
		return Promises.all(globalFsIds.stream().map(this::deleteAttachment));
	}

	Promise<Void> deleteAttachment(String globalFsId);

	Promise<Long> attachmentSize(String globalFsId);

	Promise<ChannelSupplier<ByteBuf>> loadAttachment(String globalFsId, long offset, long limit);

	default Promise<ChannelSupplier<ByteBuf>> loadAttachment(String globalFsId) {
		return loadAttachment(globalFsId, 0, -1);
	}
}
