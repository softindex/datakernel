package io.global.photos.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.registry.CodecRegistry;
import io.datakernel.common.MemSize;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.ref.RefLong;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.common.tuple.Tuple1;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.common.tuple.Tuple3;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.HttpException;
import io.datakernel.http.decoder.Decoder;
import io.datakernel.promise.Promise;
import io.global.common.CryptoUtils;
import io.global.mustache.MustacheTemplater;
import io.global.ot.session.AuthService;
import io.global.ot.session.UserId;
import io.global.ot.value.ChangeValue;
import io.global.photos.http.PublicServlet;
import io.global.photos.ot.Photo;
import io.global.photos.ot.operation.*;

import java.time.Instant;
import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.http.decoder.Decoders.ofGet;
import static io.global.Utils.isGzipAccepted;
import static io.global.ot.OTUtils.createOTRegistry;
import static io.global.ot.OTUtils.ofChangeValue;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class Utils {
	public static final Decoder<Tuple2<Integer, Integer>> PAGINATION_DECODER = Decoder.of(Tuple2::new,
			ofGet("page")
					.map(Integer::valueOf, "Cannot parse page")
					.validate(value -> value > 0, "Cannot be less or equal 0"),
			ofGet("size")
					.map(Integer::valueOf, "Cannot parse size")
					.validate(value -> value >= 0, "Cannot be less 0"));
	public static final Throwable NOT_PRIVILEGED = HttpException.ofCode(401, "Not privileged");
	public static final StructuredCodec<Set<String>> SET_CODEC = StructuredCodecs.ofSet(StructuredCodecs.STRING_CODEC);
	private static final String WHITESPACE = "^(?:\\p{Z}|\\p{C})*$";
	private static final StacklessException OVERFLOW_STREAM_LIMIT = new StacklessException(ChannelSuppliers.class, "Overflow stream limit");
	private static final int ID_SIZE = 10;
	private static StructuredCodec<ChangeValue<String>> CHANGE_VALUE_STRING_CODER = ofChangeValue(STRING_CODEC);
	private static StructuredCodec<ChangeValue<Tuple2<String, String>>> CHANGE_VALUE_TUPLE_CODER = ofChangeValue(tuple(Tuple2::new,
			Tuple2::getValue1, STRING_CODEC,
			Tuple2::getValue2, STRING_CODEC));
	public static final CodecRegistry REGISTRY = createOTRegistry()
			.with(Instant.class, LONG_CODEC.transform(Instant::ofEpochMilli, Instant::toEpochMilli))
			.with(UserId.class, registry -> tuple(UserId::new,
					UserId::getAuthService, ofEnum(AuthService.class),
					UserId::getAuthId, STRING_CODEC))
			.with(Photo.class, tuple(Photo::create,
					Photo::getDescription, STRING_CODEC,
					Photo::getTimeUpload, LONG_CODEC,
					Photo::getFilename, STRING_CODEC,
					Photo::getWidth, INT_CODEC,
					Photo::getHeight, INT_CODEC))
			.with(AlbumAddOperation.class, tuple(AlbumAddOperation::new,
					AlbumAddOperation::getAlbumId, STRING_CODEC,
					AlbumAddOperation::getTitle, STRING_CODEC,
					AlbumAddOperation::getDescription, STRING_CODEC,
					AlbumAddOperation::isRemove, BOOLEAN_CODEC))
			.with(AlbumAddPhotoOperation.class, registry -> tuple(AlbumAddPhotoOperation::new,
					AlbumAddPhotoOperation::getAlbumId, STRING_CODEC,
					AlbumAddPhotoOperation::getPhotoId, STRING_CODEC,
					AlbumAddPhotoOperation::getPhoto, registry.get(Photo.class),
					AlbumAddPhotoOperation::isRemove, BOOLEAN_CODEC))
			.with(AlbumChangeOperation.class, registry -> tuple(AlbumChangeOperation::new,
					AlbumChangeOperation::getAlbumId, STRING_CODEC,
					AlbumChangeOperation::getMetadata, CHANGE_VALUE_TUPLE_CODER))
			.with(AlbumChangePhotoOperation.class, registry -> tuple(AlbumChangePhotoOperation::new,
					AlbumChangePhotoOperation::getAlbumId, STRING_CODEC,
					AlbumChangePhotoOperation::getPhotoId, STRING_CODEC,
					AlbumChangePhotoOperation::getDescription, CHANGE_VALUE_STRING_CODER))
			.with(AlbumOperation.class, registry -> CodecSubtype.<AlbumOperation>create()
					.with(AlbumAddOperation.class, registry.get(AlbumAddOperation.class))
					.with(AlbumAddPhotoOperation.class, registry.get(AlbumAddPhotoOperation.class))
					.with(AlbumChangeOperation.class, registry.get(AlbumChangeOperation.class))
					.with(AlbumChangePhotoOperation.class, registry.get(AlbumChangePhotoOperation.class)));

	public static final StructuredCodec<Tuple3<String, String, Set<String>>> ALBUM_CODEC = REGISTRY.get(new TypeT<Tuple3<String, String, Set<String>>>() {});
	public static final StructuredCodec<Tuple2<String, Set<String>>> MOVE_PHOTOS_CODEC = REGISTRY.get(new TypeT<Tuple2<String, Set<String>>>() {});
	public static final StructuredCodec<Tuple2<String, String>> UPDATE_ALBUM_METADATA = REGISTRY.get(new TypeT<Tuple2<String, String>>() {});
	public static final StructuredCodec<Tuple1<String>> PHOTO_DESCRIPTION_CODEC = REGISTRY.get(new TypeT<Tuple1<String>>() {});

	public static AsyncServletDecorator renderErrors(MustacheTemplater templater) {
		return servlet ->
				request ->
						servlet.serveAsync(request)
								.thenEx((response, e) -> {
									if (e != null) {
										int code = e instanceof HttpException ? ((HttpException) e).getCode() : 500;
										return templater.render(code, "error", map("code", code, "message", e.getMessage()));
									}
									int code = response.getCode();
									if (code < 400) {
										return Promise.of(response);
									}
									String message = response.isBodyLoaded() ? response.getBody().getString(UTF_8) : "";
									return templater.render(code, "error", map("code", code, "message", message.isEmpty() ? null : message));
								});
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

	public static Promise<Void> validate(String param, int maxLength, String paramName, boolean required) {
		if (param == null && required || (param != null && param.matches(WHITESPACE) && required)) {
			return Promise.ofException(new ParseException(PublicServlet.class, "'" + paramName + "' POST parameter is required"));
		}
		return param != null && param.length() > maxLength ?
				Promise.ofException(new ParseException(PublicServlet.class, paramName + " is too long (" + param.length() + ">" + maxLength + ")")) :
				Promise.complete();
	}

	public static <T> T ifNull(T suspect, T defaultValue) {
		return suspect == null ? defaultValue : suspect;
	}

	public static String generateId() {
		return CryptoUtils.toHexString(CryptoUtils.randomBytes(ID_SIZE));
	}
}
