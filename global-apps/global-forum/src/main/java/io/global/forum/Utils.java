package io.global.forum;

import com.github.mustachejava.Mustache;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MediaTypes;
import io.datakernel.ot.OTSystem;
import io.datakernel.writer.ByteBufWriter;
import io.global.forum.ot.forum.ForumOTOperation;
import io.global.forum.ot.forum.ForumOTSystem;
import io.global.forum.pojo.AuthService;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.ThreadMetadata;
import io.global.forum.pojo.UserId;
import io.global.ot.map.MapOTSystem;
import io.global.ot.map.MapOperation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.Random;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpHeaders.REFERER;
import static io.global.ot.OTUtils.CHANGE_NAME_CODEC;
import static io.global.ot.OTUtils.getMapOperationCodec;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	private static final char[] CHAR_POOL = {
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C',
			'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
			'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c',
			'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
			'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
	};
	private static final Random RANDOM = new Random();

	private static final Comparator<ThreadMetadata> THREAD_METADATA_COMPARATOR = Comparator
			.comparing(ThreadMetadata::getThreadId)
			.thenComparing(ThreadMetadata::getTitle);

	private static final Comparator<Post> POST_COMPARATOR = Comparator
			.comparing(Post::getTimestamp)
			.thenComparing(Post::getContent)
			.thenComparing(comment -> comment.getAuthor().getAuthService().ordinal())
			.thenComparing(comment -> comment.getAuthor().getAuthString());

	public static final OTSystem<ForumOTOperation> CHANNEL_OT_SYSTEM = ForumOTSystem.create(THREAD_METADATA_COMPARATOR);

	public static final OTSystem<MapOperation<Long, Post>> COMMENTS_OT_SYSTEM =
			MapOTSystem.createOTSystem(POST_COMPARATOR);

	public static String generateBase62(int size) {
		StringBuilder sb = new StringBuilder(size);
		for (int i = 0; i < size; i++) {
			sb.append(CHAR_POOL[RANDOM.nextInt(CHAR_POOL.length)]);
		}
		return sb.toString();
	}

	public static Long generateCommentId() {
		return RANDOM.nextLong();
	}

	public static HttpResponse templated(Mustache mustache, @Nullable Object scope) {
		ByteBufWriter writer = new ByteBufWriter();
		mustache.execute(writer, scope);
		return HttpResponse.ok200()
				.withBody(writer.getBuf())
				.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(MediaTypes.HTML)));
	}

	public static HttpResponse templated(Mustache mustache) {
		return templated(mustache, null);
	}

	@Nullable
	public static String getFileExtension(String filename) {
		if (filename.lastIndexOf(".") != -1 && filename.lastIndexOf(".") != 0) {
			return filename.substring(filename.lastIndexOf(".") + 1);
		}
		return null;
	}

	public static HttpResponse redirect(HttpRequest request, @NotNull String to) {
		String referer = request.getHeader(REFERER);
		return HttpResponse.redirect302(referer == null ? to : referer);
	}

	// region codecs
	public static final StructuredCodec<UserId> USER_ID_CODEC = tuple(UserId::new,
			UserId::getAuthService, ofEnum(AuthService.class),
			UserId::getAuthString, STRING_CODEC);

	public static final StructuredCodec<Post> COMMENT_CODEC = tuple(Post::new,
			Post::getAuthor, USER_ID_CODEC,
			Post::getContent, STRING_CODEC,
			Post::getTimestamp, LONG_CODEC);

	public static final StructuredCodec<ThreadMetadata> THREAD_METADATA_CODEC = tuple(ThreadMetadata::new,
			ThreadMetadata::getTitle, STRING_CODEC,
			ThreadMetadata::getThreadId, LONG_CODEC);

	public static final StructuredCodec<MapOperation<String, ThreadMetadata>> METADATA_OP_CODEC = getMapOperationCodec(STRING_CODEC, THREAD_METADATA_CODEC);

	public static final StructuredCodec<MapOperation<Long, Post>> POST_OP_CODEC = getMapOperationCodec(LONG_CODEC, COMMENT_CODEC);

	public static final StructuredCodec<ForumOTOperation> FORUM_OP_CODEC = tuple(ForumOTOperation::new,
			ForumOTOperation::getNameOps, ofList(CHANGE_NAME_CODEC),
			ForumOTOperation::getNameOps, ofList(CHANGE_NAME_CODEC),
			ForumOTOperation::getMetadataOps, ofList(METADATA_OP_CODEC));

	// endregion
}
