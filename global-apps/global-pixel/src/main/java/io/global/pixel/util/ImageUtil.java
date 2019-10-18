package io.global.pixel.util;

import io.datakernel.exception.StacklessException;
import io.datakernel.util.Tuple2;
import org.imgscalr.Scalr;
import org.jetbrains.annotations.Nullable;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ImageUtil {
	public static final StacklessException UNSUPPORTED_FORMAT = new StacklessException("Unsupported photo format");
	private static final Pattern extensionPattern = Pattern.compile(".*\\.([\\w-]+)");

	@Nullable
	public static String fileFormat(String filename, String defaultExtension) {
		Matcher matcher = extensionPattern.matcher(filename);
		return matcher.find() ? matcher.group(1) : defaultExtension;
	}

	public static byte[] resize(BufferedImage image, int width, int height, String format, Scalr.Method method) throws StacklessException {
		BufferedImage resizedImage = Scalr.resize(image, method, width, height);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ImageIO.write(resizedImage, format, baos);
			return baos.toByteArray();
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
}
