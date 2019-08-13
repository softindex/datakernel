package io.global.video;

import com.github.mustachejava.Mustache;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MediaTypes;
import io.datakernel.ot.OTSystem;
import io.datakernel.writer.ByteBufWriter;
import io.global.ot.map.MapOTSystem;
import io.global.ot.map.MapOperation;
import io.global.video.pojo.Comment;
import io.global.video.pojo.VideoMetadata;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.Random;

import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;

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

	private static final Comparator<VideoMetadata> VIDEO_METADATA_COMPARATOR = Comparator
			.comparing(VideoMetadata::getTitle)
			.thenComparing(VideoMetadata::getDescription);

	private static final Comparator<Comment> COMMENT_COMPARATOR = Comparator
			.comparing(Comment::getTimestamp)
			.thenComparing(Comment::getContent)
			.thenComparing(comment -> comment.getAuthor().getAuthService().ordinal())
			.thenComparing(comment -> comment.getAuthor().getAuthString());

	public static final OTSystem<MapOperation<String, VideoMetadata>> VIDEOS_OT_SYSTEM =
			MapOTSystem.createOTSystem(VIDEO_METADATA_COMPARATOR);

	public static final OTSystem<MapOperation<Long, Comment>> COMMENTS_OT_SYSTEM =
			MapOTSystem.createOTSystem(COMMENT_COMPARATOR);

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
}
