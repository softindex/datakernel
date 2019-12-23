package io.datakernel.vlog.handler;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.MemSize;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.HttpException;
import io.datakernel.http.MultipartParser;
import io.datakernel.promise.Promise;
import io.datakernel.vlog.util.Utils;
import io.global.comm.dao.ThreadDao;
import io.global.comm.pojo.AttachmentType;
import org.imgscalr.Scalr;

import javax.imageio.ImageIO;
import java.awt.*;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static io.datakernel.vlog.handler.VideoMultipartHandler.ATTACHMENT_FIELD_NAMES;
import static io.datakernel.vlog.util.Utils.limitedSupplier;
import static io.datakernel.vlog.view.VideoHeaderView.VIDEO_VIEW_HEADER;
import static io.global.comm.pojo.AttachmentType.IMAGE;

public final class PosterMultipartHandler {
	private static final Scalr.Method DEFAULT_METHOD = Scalr.Method.ULTRA_QUALITY;
	private static final StacklessException UNSUPPORTED_FORMAT = new StacklessException("Unsupported photo format");
	private static final String POSTER = "poster";
	private static final String FORMAT = "png";

	public static MultipartParser.MultipartDataHandler create(ExecutorService executor, ThreadDao threadDao, Map<String, String> paramsMap,
															  MemSize imageLimit, Dimension dimension, Predicate<Map<String, String>> validate) {
		return MultipartParser.MultipartDataHandler.fieldsToMap(paramsMap, (fieldName, fileName) -> {
			if (fileName.isEmpty()) {
				return Promise.of(ChannelConsumers.recycling());
			}
			if (!validate.test(paramsMap)) {
				return Promise.ofException(new StacklessException("Unacceptable variable"));
			}
			AttachmentType type = ATTACHMENT_FIELD_NAMES.get(fieldName);
			if (type == null) {
				return Promise.ofException(HttpException.ofCode(400, "Unknown parameter '" + fieldName + "'"));
			}
			if (!type.equals(IMAGE)) {
				return Promise.ofException(HttpException.ofCode(400, "Unsupported non image format"));
			}
			return threadDao.deleteAttachment(VIDEO_VIEW_HEADER, POSTER)
					.then($ -> threadDao.uploadAttachment(VIDEO_VIEW_HEADER, POSTER))
					.map(consumer -> ChannelConsumer.ofSupplier(supplier -> {
						ChannelSupplier<ByteBuf> limitedSupplier = limitedSupplier(supplier, imageLimit);
						Eventloop currentEventloop = Eventloop.getCurrentEventloop();
						return Promise.ofBlockingCallable(executor,
								() -> {
									try (InputStream stream = ChannelSuppliers.channelSupplierAsInputStream(currentEventloop, limitedSupplier)) {
										return ImageIO.read(stream);
									}
								})
								.thenEx((image, e) -> e != null ? Promise.ofException(e) : image == null ? Promise.ofException(UNSUPPORTED_FORMAT) : Promise.of(image))
								.then(src -> {
									Tuple2<Integer, Integer> scaledDimension = Utils.getScaledDimension(src.getWidth(), src.getHeight(), dimension);
									return Promise.ofBlockingCallable(executor,
											() -> Utils.resize(src, scaledDimension.getValue1(), scaledDimension.getValue2(), FORMAT, DEFAULT_METHOD))
											.then(buf -> consumer.accept(buf, null));
								});
					}));
		});
	}
}
