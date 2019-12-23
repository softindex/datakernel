package io.datakernel.vlog.handler;

import io.datakernel.common.ApplicationSettings;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.global.comm.dao.ThreadDao;
import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Frame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;

import static io.datakernel.async.util.LogUtils.Level.ERROR;
import static io.datakernel.async.util.LogUtils.Level.TRACE;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.vlog.handler.VideoMultipartHandler.ORIGIN;
import static io.datakernel.vlog.util.Utils.getScaledDimension;
import static io.datakernel.vlog.view.VideoHeaderView.VIDEO_VIEW_HEADER;

public final class ThreadDaoVideoHandler implements VideoHandler {
	private static final Logger logger = LoggerFactory.getLogger(ThreadDaoVideoHandler.class);
	private static final int DEFAULT_VIDEO_CODEC = avcodec.AV_CODEC_ID_VP9;
	private static final double BITRATE = ApplicationSettings.getDouble(ThreadDaoVideoHandler.class, "bitrate", 0.8);
	private final String DEFAULT_FORMAT = "mp4";
	private final String name;
	private final ThreadDao threadDao;
	private final Executor executor;
	private final Eventloop eventloop;
	private final Path cachedPath;
	private final ProgressListener progressListener;
	private final Dimension dimension;
	private int videoCodec = DEFAULT_VIDEO_CODEC;

	public ThreadDaoVideoHandler(String name, ThreadDao threadDao, Executor executor, Eventloop eventloop,
								 Dimension dimension, Path cachedPath, ProgressListener progressListener) {
		this.name = name;
		this.threadDao = threadDao;
		this.executor = executor;
		this.eventloop = eventloop;
		this.dimension = dimension;
		this.cachedPath = cachedPath;
		this.progressListener = progressListener;
	}

	public ThreadDaoVideoHandler setVideoCodec(int videoCodec) {
		this.videoCodec = videoCodec;
		return this;
	}

	@Override
	public Promise<Void> handle(String filename) {
		Path resolvedPath = this.cachedPath.resolve(filename + "_" + name + "." + DEFAULT_FORMAT);
		return threadDao.loadAttachment(VIDEO_VIEW_HEADER, ORIGIN)
				.then(supplier -> Promise.ofBlockingRunnable(executor, () -> {
					InputStream inputStream = ChannelSuppliers.channelSupplierAsInputStream(eventloop, supplier, 1000);

					FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(inputStream);
					grabber.setAudioChannels(1);
					grabber.setVideoCodec(videoCodec);
					grabber.start();

					progressListener.trySetProgressLimit(grabber.getLengthInTime());
					if (grabber.getLengthInFrames() == 0) throw new StacklessException("Cannot process empty video");

					Frame frame = grabber.grabImage();
					if (frame == null) throw new StacklessException("Cannot parse format");
					Tuple2<Integer, Integer> scaledDimension = getScaledDimension(frame.imageWidth, frame.imageHeight, dimension);
					FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(resolvedPath.toFile(), scaledDimension.getValue1(), scaledDimension.getValue2());
					recorder.setAudioChannels(1);
					recorder.setFormat(DEFAULT_FORMAT);
					recorder.setVideoCodec(videoCodec);
					recorder.setVideoBitrate((int) (grabber.getVideoBitrate() * BITRATE));
					recorder.setAudioBitrate((int) (grabber.getAudioBitrate() * BITRATE));
					recorder.setAudioCodec(grabber.getAudioCodec());

					recorder.start();
					//noinspection ConditionalBreakInInfiniteLoop
					do {
						if (frame == null) break;
						progressListener.onProgress(name, frame.timestamp);
						recorder.record(frame);
						frame = grabber.grab();
					} while (true);
					grabber.stop();
					recorder.stop();
				}))
				.then($ -> threadDao.uploadAttachment(VIDEO_VIEW_HEADER, name)
						.then(channelConsumer -> ChannelFileReader.open(executor, resolvedPath)
								.then(reader -> reader.streamTo(channelConsumer))))
				.thenEx(($, e) -> Promise.ofBlockingCallable(executor,
						() -> {
							if (Files.exists(resolvedPath)) {
								Files.delete(resolvedPath);
							}
							if (e != null) {
								progressListener.onError(e);
								throw new StacklessException(e);
							} else {
								progressListener.onComplete();
							}
							return null;
						})
						.toVoid()
				)
				.whenComplete(toLogger(logger, TRACE, TRACE, ERROR, "handle video", name));
	}

	@Override
	public String getName() {
		return name;
	}
}
