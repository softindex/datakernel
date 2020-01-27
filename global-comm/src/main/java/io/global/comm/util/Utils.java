package io.global.comm.util;

import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredEncoder;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.codec.registry.CodecRegistry;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpRequest;
import io.datakernel.promise.Promise;
import io.global.comm.http.IpBanRequest;
import io.global.comm.ot.AppMetadata;
import io.global.comm.ot.post.operation.*;
import io.global.comm.pojo.*;
import io.global.mustache.MustacheTemplater;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.StructuredEncoder.ofObject;
import static io.datakernel.common.collection.CollectionUtils.map;
import static io.global.ot.OTUtils.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public final class Utils {
	public static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss/dd.MM.yyyy");
	private static final String WHITESPACE = "^(?:\\p{Z}|\\p{C})*$";

	private Utils() {
		throw new AssertionError();
	}

	public static CodecRegistry createCommRegistry() {
		return createOTRegistry()
				.with(Instant.class, LONG_CODEC.transform(Instant::ofEpochMilli, Instant::toEpochMilli))
				.with(InetAddress.class, BYTES_CODEC.transform(addr -> {
					try {
						return InetAddress.getByAddress(addr);
					} catch (UnknownHostException e) {
						throw new ParseException(e);
					}
				}, InetAddress::getAddress))
				.with(ThreadMetadata.class, tuple(ThreadMetadata::parse,
						ThreadMetadata::getTitle, STRING_CODEC,
						ThreadMetadata::getLastUpdate, LONG_CODEC))
				.with(UserData.class, registry -> tuple(UserData::new,
						UserData::getRole, ofEnum(UserRole.class),
						UserData::getEmail, STRING_CODEC.nullable(),
						UserData::getUsername, STRING_CODEC.nullable(),
						UserData::getFirstName, STRING_CODEC.nullable(),
						UserData::getLastName, STRING_CODEC.nullable(),
						UserData::getBanState, registry.get(BanState.class).nullable()))
				.with(IpRange.class, registry -> tuple(IpRange::new,
						IpRange::getIp, BYTES_CODEC,
						IpRange::getMask, BYTES_CODEC))
				.with(BanState.class, registry -> tuple(BanState::new,
						BanState::getBanner, registry.get(UserId.class),
						BanState::getUntil, registry.get(Instant.class),
						BanState::getReason, STRING_CODEC))
				.with(IpBanState.class, registry -> tuple(IpBanState::new,
						IpBanState::getBanState, registry.get(BanState.class),
						IpBanState::getIpRange, registry.get(IpRange.class)))
				.with(AddPost.class, registry -> tuple(AddPost::new,
						AddPost::getPostId, STRING_CODEC,
						AddPost::getParentId, STRING_CODEC.nullable(),
						AddPost::getAuthor, registry.get(UserId.class),
						AddPost::getInitialTimestamp, LONG_CODEC,
						AddPost::isRemove, BOOLEAN_CODEC))
				.with(ChangeAttachments.class, registry -> tuple(ChangeAttachments::new,
						ChangeAttachments::getPostId, STRING_CODEC,
						ChangeAttachments::getFilename, STRING_CODEC,
						ChangeAttachments::getAttachmentType, ofEnum(AttachmentType.class),
						ChangeAttachments::getTimestamp, LONG_CODEC,
						ChangeAttachments::isRemove, BOOLEAN_CODEC))
				.with(ChangeContent.class, registry -> tuple(ChangeContent::new,
						ChangeContent::getPostId, STRING_CODEC,
						ChangeContent::getChangeContent, ofChangeValue(STRING_CODEC)))
				.with(ChangeLastEditTimestamp.class, registry -> tuple(ChangeLastEditTimestamp::new,
						ChangeLastEditTimestamp::getPostId, STRING_CODEC,
						ChangeLastEditTimestamp::getPrevTimestamp, LONG_CODEC,
						ChangeLastEditTimestamp::getNextTimestamp, LONG_CODEC))
				.with(ChangeRating.class, registry -> tuple(ChangeRating::new,
						ChangeRating::getPostId, STRING_CODEC,
						ChangeRating::getUserId, registry.get(UserId.class),
						ChangeRating::getSetRating, getSetValueCodec(registry.get(Rating.class))))
				.with(DeletePost.class, registry -> tuple(DeletePost::new,
						DeletePost::getPostId, STRING_CODEC,
						DeletePost::getDeletedBy, registry.get(UserId.class),
						DeletePost::getTimestamp, LONG_CODEC,
						DeletePost::isDelete, BOOLEAN_CODEC))
				.with(PostChangesOperation.class, registry -> tuple(PostChangesOperation::new,
						PostChangesOperation::getChangeContentOps, ofList(registry.get(ChangeContent.class)),
						PostChangesOperation::getChangeAttachmentsOps, ofList(registry.get(ChangeAttachments.class)),
						PostChangesOperation::getChangeRatingOps, ofList(registry.get(ChangeRating.class)),
						PostChangesOperation::getDeletePostOps, ofList(registry.get(DeletePost.class)),
						PostChangesOperation::getChangeLastEditTimestamps, ofList(registry.get(ChangeLastEditTimestamp.class))))
				.with(ThreadOperation.class, registry -> CodecSubtype.<ThreadOperation>create()
						.with(AddPost.class, registry.get(AddPost.class))
						.with(PostChangesOperation.class, registry.get(PostChangesOperation.class)))
				.with(AppMetadata.class, registry -> tuple(AppMetadata::new,
						AppMetadata::getTitle, STRING_CODEC.nullable(),
						AppMetadata::getDescription, STRING_CODEC.nullable()))
				.with(IpBanRequest.class, registry -> tuple(IpBanRequest::new,
						IpBanRequest::getRange, registry.get(IpRange.class),
						IpBanRequest::getUntil, registry.get(Instant.class),
						IpBanRequest::getDescription, STRING_CODEC));
	}

	public static final CodecFactory REGISTRY = createCommRegistry();

	private static final char[] CHAR_POOL = {
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'
	};

	private static final StructuredEncoder<Post> POST_SIMPLE_ENCODER = (out, post) -> {
		StructuredCodec<UserId> userIdCodec = REGISTRY.get(UserId.class);
		out.writeKey("author", userIdCodec, post.getAuthor());
		out.writeKey("created", LONG_CODEC, post.getInitialTimestamp());
		out.writeKey("content", STRING_CODEC, post.getContent());
		out.writeKey("attachments", ofMap(STRING_CODEC, REGISTRY.get(AttachmentType.class)), post.getAttachments());
		out.writeKey("deletedBy", userIdCodec.nullable(), post.getDeletedBy());
		out.writeKey("edited", LONG_CODEC, post.getLastEditTimestamp());
		out.writeKey("ratings", REGISTRY.get(new TypeT<Map<Rating, Set<UserId>>>() {}), post.getRatings());
	};

	private static final StructuredEncoder<Tuple2<Map<String, Post>, String>> RECURSIVE_POST_ENCODER;

	static {
		LazyEncoder<Tuple2<Map<String, Post>, String>> lazyPostEncoder = new LazyEncoder<>();
		RECURSIVE_POST_ENCODER = ofObject((out, data) -> {
			Map<String, Post> posts = data.getValue1();
			String ourRootId = data.getValue2();
			Post post = posts.get(ourRootId);
			List<Post> children = post.getChildren();

			List<Tuple2<Map<String, Post>, String>> childrenEntries = posts.entrySet().stream()
					.filter(postEntry -> children.contains(postEntry.getValue()))
					.map(postEntry -> new Tuple2<>(posts, postEntry.getKey()))
					.collect(toList());

			out.writeKey("id", STRING_CODEC, ourRootId);
			POST_SIMPLE_ENCODER.encode(out, post);
			out.writeKey("children", StructuredEncoder.ofList(lazyPostEncoder), childrenEntries);
		});
		lazyPostEncoder.realize(RECURSIVE_POST_ENCODER);
	}

	private static final StructuredEncoder<Object> EMPTY_OBJECT_ENCODER = StructuredEncoder.ofObject();
	public static final StructuredEncoder<Map<String, Post>> POSTS_ENCODER_ROOT = postsEncoder("root");
	public static final StructuredEncoder<Post> POST_ENCODER = StructuredEncoder.ofObject(POST_SIMPLE_ENCODER);

	public static StructuredEncoder<Map<String, Post>> postsEncoder(String startingId) {
		return (out, posts) -> {
			Post root = posts.get(startingId);
			if (root == null) {
				EMPTY_OBJECT_ENCODER.encode(out, emptyMap());
			} else {
				RECURSIVE_POST_ENCODER.encode(out, new Tuple2<>(posts, startingId));
			}
		};
	}

	private static final Random RANDOM = new Random();
	private static final int ID_SIZE = 10;

	public static String generateId() {
		StringBuilder sb = new StringBuilder(ID_SIZE);
		for (int i = 0; i < 10; i++) {
			sb.append(CHAR_POOL[RANDOM.nextInt(CHAR_POOL.length)]);
		}
		return sb.toString();
	}

	public static AsyncServletDecorator renderErrors(MustacheTemplater templater) {
		return servlet ->
				request ->
						servlet.serve(request).get()
								.thenEx((response, e) -> {
									if (e != null) {
										int code;
										if (e instanceof HttpException) {
											code = ((HttpException) e).getCode();
										} else if (e instanceof ParseException) {
											code = 400;
										} else {
											code = 500;
										}
										return templater.render(code, "error", map("code", code, "message", e.getMessage()));
									}
									int code = response.getCode();
									if (code < 400) {
										return Promise.of(response);
									}
									String message = response.isBodyLoaded() ? response.getBody().asString(UTF_8) : "";
									return templater.render(code, "error", map("code", code, "message", message.isEmpty() ? null : message));
								});
	}

	public static int getUnsignedInt(HttpRequest request, String queryParameter, int defaultValue) {
		try {
			String parameter = request.getQueryParameter(queryParameter);
			if (parameter == null) {
				return defaultValue;
			}
			return Integer.parseUnsignedInt(parameter);
		} catch (NumberFormatException ignored) {
			return defaultValue;
		}
	}

	@Contract("_, _, _, true -> !null")
	@Nullable
	private static String getPostParameterImpl(Map<String, String> postParameters, String name, int maxLength, boolean required) throws ParseException {
		String parameter = postParameters.get(name);
		if (parameter == null || parameter.matches(WHITESPACE)) {
			if (required) {
				throw new ParseException(Utils.class, "'" + name + "' POST parameter is required");
			}
			return null;
		}
		String trimmed = parameter.trim();
		if (trimmed.length() > maxLength) {
			throw new ParseException(Utils.class, "'" + name + "' POST parameter is too long (" + trimmed.length() + ">" + maxLength + ")");
		}
		return trimmed;
	}

	public static String getPostParameter(Map<String, String> postParameters, String name, int maxLength) throws ParseException {
		return getPostParameterImpl(postParameters, name, maxLength, true);
	}

	public static String getOptionalPostParameter(Map<String, String> postParameters, String name, int maxLength) throws ParseException {
		return getPostParameterImpl(postParameters, name, maxLength, false);
	}

	public static String getRequiredPostParameter(HttpRequest request, String name) throws ParseException {
		String parameter = request.getPostParameter(name);
		if (parameter == null) {
			throw new ParseException(Utils.class, "'" + name + "' POST parameter is required");
		}
		return parameter;
	}

	private static byte parseByte(HttpRequest request, String name) throws ParseException {
		String parameter = request.getPostParameter(name);
		if (parameter == null) {
			throw new ParseException("'" + name + "' POST parameter is required");
		}
		try {
			short s = Short.parseShort(parameter);
			if (s < 0 || s > 255) {
				throw new ParseException("POST parameter '" + name + "' parsing error: byte not in range 0-255");
			}
			return (byte) (s & 0xFF);
		} catch (NumberFormatException e) {
			throw new ParseException(e.getMessage());
		}
	}

	public static byte[] parse4Bytes(HttpRequest request, String prefix) throws ParseException {
		return new byte[]{
				parseByte(request, prefix + "_0"),
				parseByte(request, prefix + "_1"),
				parseByte(request, prefix + "_2"),
				parseByte(request, prefix + "_3")
		};
	}

	public static String formatInstant(@Nullable Instant timestamp) {
		return timestamp == null ? "" : timestamp.atZone(ZoneId.systemDefault()).format(DATE_TIME_FORMAT);
	}

	public static String formatInstant(long timestamp) {
		return timestamp == -1 ? "" : Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).format(DATE_TIME_FORMAT);
	}
}
