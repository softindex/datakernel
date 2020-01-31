package io.global.photos.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.MemSize;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.tuple.Tuple2;
import org.imgscalr.Scalr;
import org.jetbrains.annotations.NotNull;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ImageUtil {
	public static final StacklessException UNSUPPORTED_FORMAT = new StacklessException("Unsupported photo format");
	private static final Pattern extensionPattern = Pattern.compile(".*\\.([\\w-]+)");

	@NotNull
	public static String fileFormat(String filename, String defaultExtension) {
		Matcher matcher = extensionPattern.matcher(filename);
		return matcher.find() ? matcher.group(1) : defaultExtension;
	}

	public static ByteBuf resize(BufferedImage image, int width, int height, String format, Scalr.Method method) throws StacklessException {
		BufferedImage resizedImage = Scalr.resize(image, method, width, height);
		ByteBufOutputStream baos = new ByteBufOutputStream();
		try {
			ImageIO.write(resizedImage, format, baos);
			baos.flush();
			return baos.getBuf();
		} catch (IOException e) {
			throw new StacklessException(e);
		}
	}

	public static Tuple2<Integer, Integer> getScaledDimension(int imageX, int imageY, Dimension boundaryDimension) {
		double widthRatio = boundaryDimension.getWidth() / imageX;
		double heightRatio = boundaryDimension.getHeight() / imageY;
		double ratio = Math.min(widthRatio, heightRatio);
		return new Tuple2<>((int) (imageX * ratio), (int) (imageY * ratio));
	}

	private static class ByteBufOutputStream extends OutputStream {
		public static final MemSize INITIAL_BUF_SIZE = MemSize.kilobytes(2);

		private final MemSize initialBufSize;
		private ByteBuf byteBuf;
		private ByteBuf collector;

		public ByteBufOutputStream() {
			this(INITIAL_BUF_SIZE);
		}

		public ByteBufOutputStream(MemSize initialSize) {
			this.initialBufSize = initialSize;
			byteBuf = ByteBufPool.allocate(initialSize);
			collector = ByteBufPool.allocate(initialSize);
		}

		@Override
		public void write(int b) {
			if (collector.writeRemaining() > 0) {
				collector.writeByte((byte) b);
			} else {
				this.byteBuf = ByteBufPool.append(byteBuf, collector);
				collector = ByteBufPool.allocate(initialBufSize);
			}
		}

		@Override
		public void write(@NotNull byte[] bytes, int off, int len) {
			this.byteBuf = ByteBufPool.append(byteBuf, bytes, off, len);
		}

		public ByteBuf getBuf() {
			return byteBuf;
		}

		@Override
		public void flush() {
			this.byteBuf = ByteBufPool.append(byteBuf, collector);
		}
	}
}
