package io.global.forum.http;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.http.HttpException;
import io.datakernel.http.MultipartParser.MultipartDataHandler;
import io.global.comm.dao.ThreadDao;
import io.global.comm.pojo.Attachment;
import io.global.comm.pojo.AttachmentType;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class AttachmentDataHandler {
	private static final Map<String, AttachmentType> ATTACHMENT_FIELD_NAMES = Arrays.stream(AttachmentType.values())
			.collect(Collectors.toMap(type -> type.toString().toLowerCase() + "_attachment", Function.identity()));

	public static MultipartDataHandler create(ThreadDao threadDao, Map<String, String> paramMap, Map<String, Attachment> attachmentMap) {
		return MultipartDataHandler.fieldsToMap(paramMap, (fieldName, fileName) -> {
			if (fileName.isEmpty()) {
				return Promise.of(ChannelConsumers.recycling());
			}
			AttachmentType type = ATTACHMENT_FIELD_NAMES.get(fieldName);
			if (type == null) {
				return Promise.ofException(HttpException.ofCode(400, "Unknown parameter '" + fieldName + "'"));
			}
			return threadDao.uploadAttachment()
					.map(uploader -> {
						attachmentMap.put(uploader.getGlobalFsId(), new Attachment(type, fileName));
						return uploader.getUploader();
					});
		});
	}
}
