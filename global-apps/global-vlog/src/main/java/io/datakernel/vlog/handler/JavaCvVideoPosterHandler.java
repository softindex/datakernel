package io.datakernel.vlog.handler;

import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.vlog.util.Utils;
import io.global.comm.dao.ThreadDao;
import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.imgscalr.Scalr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.util.concurrent.Executor;

import static io.datakernel.async.util.LogUtils.Level.ERROR;
import static io.datakernel.async.util.LogUtils.Level.TRACE;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.vlog.view.VideoHeaderView.VIDEO_VIEW_HEADER;

public class JavaCvVideoPosterHandler implements VideoPosterHandler {
	private static final Logger logger = LoggerFactory.getLogger(VideoPosterHandler.class);
	private static final Scalr.Method DEFAULT_METHOD = Scalr.Method.ULTRA_QUALITY;
	private static final int DEFAULT_VIDEO_CODEC = avcodec.AV_CODEC_ID_VP9;
	private static final String POSTER = "poster";
	private static final String FORMAT = "png";
	private static Java2DFrameConverter converter = new Java2DFrameConverter();
	private final ThreadDao threadDao;
	private final Executor executor;
	private final Eventloop eventloop;
	private final Dimension dimension;
	private ProgressListener listener;
	private int videoCodec = DEFAULT_VIDEO_CODEC;

	public JavaCvVideoPosterHandler(ThreadDao threadDao, Executor executor, Eventloop eventloop, Dimension dimension, ProgressListener listener) {
		this.threadDao = threadDao;
		this.executor = executor;
		this.eventloop = eventloop;
		this.dimension = dimension;
		this.listener = listener;
	}

	@Override
	public Promise<Void> handle(String videoSourceName) {
		return getFirstFrame(videoSourceName)
				.then(frame -> Promise.ofBlockingCallable(executor,
						() -> {
							if (frame == null) {
								throw new StacklessException("Cannot get frame");
							}
							BufferedImage bufferedImage = converter.convert(frame);
							Tuple2<Integer, Integer> scaledDimension = Utils.getScaledDimension(bufferedImage.getWidth(), bufferedImage.getHeight(), dimension);
							return Utils.resize(bufferedImage, scaledDimension.getValue1(), scaledDimension.getValue2(), FORMAT, DEFAULT_METHOD);
						})
				)
				.then(image -> threadDao.uploadAttachment(VIDEO_VIEW_HEADER, POSTER)
						.then(channelConsumer -> channelConsumer.accept(image, null)))
				.whenComplete(($, ex) -> {
					if (ex == null) {
						listener.onComplete();
					} else {
						listener.onError(ex);
					}
				});
	}

	private Promise<Frame> getFirstFrame(String filename) {
		return threadDao.loadAttachment(VIDEO_VIEW_HEADER, filename)
				.then(supplier -> Promise.ofBlockingCallable(executor, () -> {
							InputStream inputStream = ChannelSuppliers.channelSupplierAsInputStream(eventloop, supplier, 1000);
							FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(inputStream);
							grabber.setAudioChannels(1);
							grabber.setVideoCodec(videoCodec);

							grabber.start();
							if (grabber.getLengthInFrames() <= 0) {
								grabber.stop();
								throw new StacklessException("Can grab no frames");
							}
							Frame frame = grabber.grabImage();
							grabber.stop();
							return frame;
						})
				)
				.whenComplete(toLogger(logger, TRACE, TRACE, ERROR, "handle poster", filename));
	}
}
