package io.datakernel.vlog.handler;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.http.HttpException;
import io.datakernel.http.MultipartParser;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.vlog.util.Utils;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.pojo.AttachmentType;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.datakernel.common.collection.CollectionUtils.set;
import static io.datakernel.vlog.view.VideoHeaderView.VIDEO_VIEW_HEADER;
import static io.global.comm.pojo.AttachmentType.IMAGE;
import static io.global.comm.pojo.AttachmentType.VIDEO;

public final class VideoMultipartHandler {
	public static final String ORIGIN = "origin";
	public static final Map<String, AttachmentType> ATTACHMENT_FIELD_NAMES = Arrays.stream(AttachmentType.values())
			.collect(Collectors.toMap(type -> type.toString().toLowerCase() + "_attachment", Function.identity()));
	private final static Set<String> SUPPORTED_FORMATS = set("mp4", "mov");
	private static final String POSTER = "poster";
	private final Map<String, AsyncSupplier<Void>> pendingTasks = new LinkedHashMap<>();
	private Promise<Void> currentTask = Promise.complete();

	public MultipartParser.MultipartDataHandler create(String threadId, ThreadDao threadDao, Map<String, AttachmentType> attachmentMap,
													   List<VideoHandler> handlers, VideoPosterHandler posterHandler, CommDao commDao,
													   Predicate<Map<String, String>> paramsValid, ExecutorService currentExecutor) {
		HashMap<String, String> paramsMap = new HashMap<>();
		return MultipartParser.MultipartDataHandler.fieldsToMap(paramsMap, (fieldName, fileName) -> {
			if (fileName.isEmpty()) {
				return Promise.of(ChannelConsumers.recycling());
			}
			if (!paramsValid.test(paramsMap)) {
				return Promise.ofException(new StacklessException("Unacceptable params"));
			}
			AttachmentType type = ATTACHMENT_FIELD_NAMES.get(fieldName);
			if (type == null) {
				return Promise.ofException(HttpException.ofCode(400, "Unknown parameter '" + fieldName + "'"));
			}
			String format = Utils.fileFormat(fileName, "");
			if (!SUPPORTED_FORMATS.contains(format) || !type.equals(VIDEO)) {
				return Promise.ofException(HttpException.ofCode(400, "Unsupported video format"));
			}

			return uploadVideo(threadId, threadDao, ORIGIN, posterHandler, attachmentMap, handlers, commDao, currentExecutor);
		});
	}

	private Promise<ChannelConsumer<ByteBuf>> uploadVideo(String threadId, ThreadDao threadDao, String fileName, VideoPosterHandler posterHandler,
														  Map<String, AttachmentType> attachmentMap, List<VideoHandler> handlers, CommDao commDao,
														  ExecutorService currentExecutor) {
		return threadDao.uploadAttachment(VIDEO_VIEW_HEADER, fileName)
				.map(uploader -> {
					handlers.forEach(handler -> attachmentMap.put(handler.getName(), VIDEO));
					attachmentMap.put(fileName, VIDEO);
					attachmentMap.put(POSTER, IMAGE);
					return uploader.withAcknowledgement(done -> done
							.whenResult($1 -> {
								pendingTasks.put(threadId,
										() -> {
											Promise<Void> handleVideos = Promises.all(handlers
													.stream()
													.map(videoHandler -> videoHandler.handle(threadId))
											);
											return handleVideos
													.then($ -> posterHandler.handle(fileName))
													.thenEx(($, e) -> {
														currentExecutor.shutdown();
														pendingTasks.remove(threadId);
														return e != null ?
																removeAttachments(threadId, fileName, threadDao, commDao, handlers) :
																defineNextTask();
													});
										}
								);
								currentTask = tryExecuteNextTask();
							}));
				});
	}

	private Promise<Void> removeAttachments(String threadId, String fileName, ThreadDao threadDao, CommDao commDao, List<VideoHandler> handlers) {
		Promise<Void> all = Promises.all(handlers
				.stream()
				.map(videoHandler -> threadDao.deleteAttachment(VIDEO_VIEW_HEADER, fileName)));
		return all
				.thenEx(($, ex) -> threadDao.deleteAttachment(VIDEO_VIEW_HEADER, ORIGIN))
				.thenEx(($, ex) -> threadDao.deleteAttachment(VIDEO_VIEW_HEADER, POSTER))
				.thenEx(($, ex) -> commDao.getThreads("root")
						.remove(threadId)
				);
	}

	private Promise<Void> defineNextTask() {
		Optional<Map.Entry<String, AsyncSupplier<Void>>> optionalEntry = pendingTasks.entrySet().stream().findFirst();
		if (optionalEntry.isPresent()) {
			Map.Entry<String, AsyncSupplier<Void>> entry = optionalEntry.get();
			AsyncSupplier<Void> task = entry.getValue();
			return task.get();
		}
		return Promise.complete();
	}

	private Promise<Void> tryExecuteNextTask() {
		return currentTask.then($ -> {
			Optional<Map.Entry<String, AsyncSupplier<Void>>> optionalEntry = pendingTasks
					.entrySet()
					.stream()
					.findAny();
			if (optionalEntry.isPresent()) {
				Map.Entry<String, AsyncSupplier<Void>> entry = optionalEntry.get();
				AsyncSupplier<Void> task = entry.getValue();
				return task.get();
			}
			return Promise.complete();
		});
	}

	public List<String> pendingView() {
		return new ArrayList<>(pendingTasks.keySet());
	}
}
