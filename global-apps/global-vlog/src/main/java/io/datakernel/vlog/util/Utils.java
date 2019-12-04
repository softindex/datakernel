package io.datakernel.vlog.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.registry.CodecRegistry;
import io.datakernel.common.MemSize;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.ref.RefLong;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.promise.Promise;
import io.datakernel.vlog.adapter.ByteBufOutputStream;
import io.datakernel.vlog.ot.VlogMetadata;
import org.imgscalr.Scalr;
import org.jetbrains.annotations.NotNull;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;

public final class Utils {
	private static final StacklessException OVERFLOW_STREAM_LIMIT = new StacklessException(ChannelSuppliers.class, "Overflow stream limit");
	public static final CodecRegistry REGISTRY = ((CodecRegistry) io.global.comm.util.Utils.REGISTRY)
			.with(VlogMetadata.class, registry -> tuple(VlogMetadata::new,
					VlogMetadata::getTitle, STRING_CODEC,
					VlogMetadata::getDescription, STRING_CODEC));

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

	public static ChannelSupplier<ByteBuf> limitedSupplier(ChannelSupplier<ByteBuf> supplier, MemSize limit) {
		RefLong lim = new RefLong(limit.toLong());
		return supplier.mapAsync(byteBuf -> {
			long currentLimit = lim.dec(byteBuf.readRemaining());
			if (currentLimit < 0) {
				byteBuf.recycle();
				return Promise.ofException(OVERFLOW_STREAM_LIMIT);
			}
			return Promise.of(byteBuf);
		});
	}

	private static final Pattern extensionPattern = Pattern.compile(".*\\.([\\w-]+)");

	@NotNull
	public static String fileFormat(String filename, String defaultExtension) {
		Matcher matcher = extensionPattern.matcher(filename);
		return matcher.find() ? matcher.group(1) : defaultExtension;
	}

	public static Tuple2<Integer, Integer> getScaledDimension(int imageX, int imageY, Dimension boundaryDimension) {
		double widthRatio = boundaryDimension.getWidth() / imageX;
		double heightRatio = boundaryDimension.getHeight() / imageY;
		double ratio = Math.min(widthRatio, heightRatio);
		return new Tuple2<>((int) (imageX * ratio), (int) (imageY * ratio));
	}
}
