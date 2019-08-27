package io.global.forum;

import com.github.mustachejava.Mustache;
import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredEncoder;
import io.datakernel.codec.StructuredOutput;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MediaTypes;
import io.datakernel.util.Tuple2;
import io.datakernel.writer.ByteBufWriter;
import io.global.forum.ot.post.operation.AddPost;
import io.global.forum.ot.post.operation.PostChangesOperation;
import io.global.forum.ot.post.operation.PostOperation;
import io.global.forum.pojo.*;
import io.global.ot.map.MapOperation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.StructuredEncoder.ofList;
import static io.datakernel.codec.StructuredEncoder.ofObject;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpHeaders.REFERER;
import static io.global.ot.OTUtils.getMapOperationCodec;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

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

	public static String generateBase62(int size) {
		StringBuilder sb = new StringBuilder(size);
		for (int i = 0; i < size; i++) {
			sb.append(CHAR_POOL[RANDOM.nextInt(CHAR_POOL.length)]);
		}
		return sb.toString();
	}

	public static Long generateId() {
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

	public static HttpResponse redirect(HttpRequest request, @NotNull String to) {
		String referer = request.getHeader(REFERER);
		return HttpResponse.redirect302(referer == null ? to : referer);
	}

	// region codecs

	public static class LazyEncoder<T> implements StructuredEncoder<T> {

		private StructuredEncoder<T> peer = null;

		public void realize(StructuredEncoder<T> peer) {
			this.peer = peer;
		}

		@Override
		public void encode(StructuredOutput out, T item) {
			peer.encode(out, item);
		}
	}

	public static final StructuredCodec<UserId> USER_ID_CODEC = tuple(UserId::new,
			UserId::getAuthService, ofEnum(AuthService.class),
			UserId::getId, STRING_CODEC);

	public static final StructuredCodec<UserData> USER_DATA_CODEC = tuple(UserData::new,
			UserData::getEmail, STRING_CODEC.nullable(),
			UserData::getName, STRING_CODEC.nullable(),
			UserData::getRole, ofEnum(UserRole.class));

	public static final StructuredCodec<MapOperation<UserId, UserData>> USER_DATA_OP_CODEC = getMapOperationCodec(USER_ID_CODEC, USER_DATA_CODEC);

	public static final StructuredCodec<PostOperation> POST_OPERATION_CODEC = CodecSubtype.<PostOperation>create()
			.with(AddPost.class, AddPost.CODEC)
			.with(PostChangesOperation.class, PostChangesOperation.CODEC);


	public static final StructuredEncoder<Tuple2<Map<Long, Post>, Long>> RECURSIVE_POST_ENCODER;

	public static final StructuredEncoder<Post> POST_SIMPLE_ENCODER = (out, post) -> {
		out.writeKey("author", UserId.CODEC, post.getAuthor());
		out.writeKey("created", LONG_CODEC, post.getInitialTimestamp());
		out.writeKey("content", STRING_CODEC, post.getContent());
		out.writeKey("attachments", ofMap(STRING_CODEC, Attachment.CODEC), post.getAttachments());
		out.writeKey("deletedBy", UserId.CODEC.nullable(), post.getDeletedBy());
		out.writeKey("edited", LONG_CODEC, post.getLastEditTimestamp());
		out.writeKey("likes", ofSet(UserId.CODEC), post.getLikes());
		out.writeKey("dislikes", ofSet(UserId.CODEC), post.getDislikes());
	};

	static {
		LazyEncoder<Tuple2<Map<Long, Post>, Long>> lazyPostEncoder = new LazyEncoder<>();
		RECURSIVE_POST_ENCODER = ofObject((out, data) -> {
			Map<Long, Post> posts = data.getValue1();
			Long ourRootId = data.getValue2();
			Post post = posts.get(ourRootId);
			List<Post> children = post.getChildren();

			List<Tuple2<Map<Long, Post>, Long>> childrenEntries = posts.entrySet().stream()
					.filter(postEntry -> children.contains(postEntry.getValue()))
					.map(postEntry -> new Tuple2<>(posts, postEntry.getKey()))
					.collect(toList());

			out.writeKey("id", LONG_CODEC, ourRootId);
			POST_SIMPLE_ENCODER.encode(out, post);
			out.writeKey("children", ofList(lazyPostEncoder), childrenEntries);
		});
		lazyPostEncoder.realize(RECURSIVE_POST_ENCODER);
	}

	public static final StructuredEncoder<Object> EMPTY_OBJECT_ENCODER = StructuredEncoder.ofObject();

	public static final StructuredEncoder<Map<Long, Post>> POSTS_ENCODER_ROOT = postsEncoder(0L);

	public static final StructuredEncoder<Post> POST_ENCODER = StructuredEncoder.ofObject(POST_SIMPLE_ENCODER);

	public static StructuredEncoder<Map<Long, Post>> postsEncoder(long startingId) {
		return (out, posts) -> {
			Post root = posts.get(startingId);
			if (root == null) {
				EMPTY_OBJECT_ENCODER.encode(out, emptyMap());
			} else {
				RECURSIVE_POST_ENCODER.encode(out, new Tuple2<>(posts, startingId));
			}
		};
	}

}
