package io.global.comm.http;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.exception.StacklessException;
import io.datakernel.http.HttpException;
import io.datakernel.http.MultipartParser.MultipartDataHandler;
import io.global.comm.dao.ThreadDao;
import io.global.comm.pojo.AttachmentType;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.datakernel.util.CollectionUtils.union;
import static java.util.Collections.emptySet;

public final class AttachmentDataHandler {
	private static final Map<String, AttachmentType> ATTACHMENT_FIELD_NAMES = Arrays.stream(AttachmentType.values())
			.collect(Collectors.toMap(type -> type.toString().toLowerCase() + "_attachment", Function.identity()));

	public static MultipartDataHandler create(ThreadDao threadDao, String postId, Map<String, String> paramMap,
											  Map<String, AttachmentType> attachmentMap, boolean deduplicate) {
		return create(threadDao, postId, emptySet(), paramMap, attachmentMap, deduplicate);
	}

	public static MultipartDataHandler create(ThreadDao threadDao, String postId, Set<String> existing, Map<String, String> paramMap,
											  Map<String, AttachmentType> attachmentMap, boolean deduplicate) {
		return MultipartDataHandler.fieldsToMap(paramMap, (fieldName, fileName) -> {
			if (fileName.isEmpty()) {
				return Promise.of(ChannelConsumers.recycling());
			}

			if (existing.contains(fileName) || attachmentMap.containsKey(fileName)) {
				if (deduplicate) {
					fileName = deduplicate(union(existing, attachmentMap.keySet()), fileName);
				} else {
					return Promise.ofException(new StacklessException(AttachmentDataHandler.class, "Attachment with filename " + fileName + " already uploaded"));
				}
			}

			AttachmentType type = ATTACHMENT_FIELD_NAMES.get(fieldName);
			if (type == null) {
				return Promise.ofException(HttpException.ofCode(400, "Unknown parameter '" + fieldName + "'"));
			}
			String finalFileName = fileName;
			return threadDao.uploadAttachment(postId, fileName)
					.map(uploader -> uploader
							.withAcknowledgement(done -> done
									.whenResult($1 -> attachmentMap.put(finalFileName, type))));
		});
	}

	private static String deduplicate(Set<String> existing, String filename) {
		StringBuilder sb = new StringBuilder(filename);

		int dotIndex = filename.lastIndexOf(".");
		int index = dotIndex == -1 ? filename.length() : dotIndex;
		sb.insert(index++, "(1)");
		int counter = 1;

		String result = sb.toString();
		while (existing.contains(result)) {
			sb.replace(index, index + (int) (Math.log10(counter) + 1), Integer.toString(++counter));
			result = sb.toString();
		}
		return result;
	}

}
