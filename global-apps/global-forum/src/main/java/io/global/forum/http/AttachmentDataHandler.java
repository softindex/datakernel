package io.global.forum.http;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.http.HttpException;
import io.datakernel.http.MultipartParser.MultipartDataHandler;
import io.global.forum.dao.AttachmentDao;
import io.global.forum.pojo.Attachment;
import io.global.forum.pojo.AttachmentType;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class AttachmentDataHandler {
	private static final Map<String, AttachmentType> ATTACHMENT_FIELD_NAMES = Arrays.stream(AttachmentType.values())
			.collect(Collectors.toMap(type -> type.toString().toLowerCase() + "Attachment", Function.identity()));

	public static MultipartDataHandler create(AttachmentDao attachmentDao, Map<String, String> paramMap, Map<String, Attachment> attachmentMap) {
		return MultipartDataHandler.fieldsToMap(paramMap, (fieldName, fileName) -> {
			if (fileName.isEmpty()) {
				return Promise.of(ChannelConsumers.recycling());
			}
			AttachmentType type = ATTACHMENT_FIELD_NAMES.get(fieldName);
			if (type == null) {
				return Promise.ofException(HttpException.ofCode(400, "Unknown parameter"));
			}
			return attachmentDao.generateGlobalFsId()
					.then(globalFsId -> attachmentDao.uploadAttachment(globalFsId)
							.whenComplete(() -> attachmentMap.put(globalFsId, new Attachment(type, fileName))));
		});
	}

}
