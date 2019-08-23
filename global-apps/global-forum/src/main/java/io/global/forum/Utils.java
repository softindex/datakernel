package io.global.forum;

import com.github.mustachejava.Mustache;
import io.datakernel.codec.*;
import io.datakernel.exception.ParseException;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MediaTypes;
import io.datakernel.writer.ByteBufWriter;
import io.global.forum.ot.post.ThreadOTState;
import io.global.forum.ot.post.operation.AddPost;
import io.global.forum.ot.post.operation.PostChangesOperation;
import io.global.forum.ot.post.operation.PostOperation;
import io.global.forum.pojo.Attachment;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.UserId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Random;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.StructuredEncoder.ofList;
import static io.datakernel.codec.StructuredEncoder.ofObject;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpHeaders.REFERER;
import static java.util.stream.Collectors.toMap;

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

	public static class LazyCodec<T> implements StructuredCodec<T> {

		private StructuredCodec<T> peer = null;

		public void realize(StructuredCodec<T> peer) {
			this.peer = peer;
		}

		@Override
		public T decode(StructuredInput in) throws ParseException {
			return peer.decode(in);
		}

		@Override
		public void encode(StructuredOutput out, T item) {
			peer.encode(out, item);
		}
	}

	public static final StructuredCodec<PostOperation> POST_OPERATION_CODEC = CodecSubtype.<PostOperation>create()
			.with(AddPost.class, AddPost.CODEC)
			.with(PostChangesOperation.class, PostChangesOperation.CODEC);

	public static final StructuredEncoder<ThreadOTState> STATE_ENCODER = (out, state) -> {
		Map<Long, Post> posts = state.getPostsView();
		if (posts.isEmpty()) {
			StructuredEncoder.ofObject().encode(out, posts);
			return;
		}
		// TODO eduard: find something better
		Map<Post, Long> inverted = posts.entrySet().stream()
				.collect(toMap(Map.Entry::getValue, Map.Entry::getKey));
		Post root = posts.get(0L);
		LazyCodec<Post> lazyPostCodec = new LazyCodec<>();

		StructuredEncoder<Post> postEncoder = ofObject((postOut, post) -> {
			postOut.writeKey("id", LONG_CODEC, inverted.get(post));
			postOut.writeKey("author", UserId.CODEC, post.getAuthor());
			postOut.writeKey("created", LONG_CODEC, post.getInitialTimestamp());
			postOut.writeKey("content", STRING_CODEC, post.getContent());
			postOut.writeKey("attachments", ofMap(STRING_CODEC, Attachment.CODEC), post.getAttachments());
			postOut.writeKey("deletedBy", UserId.CODEC.nullable(), post.getDeletedBy());
			postOut.writeKey("edited", LONG_CODEC, post.getLastEditTimestamp());
			postOut.writeKey("likes", ofSet(UserId.CODEC), post.getLikes());
			postOut.writeKey("dislikes", ofSet(UserId.CODEC), post.getDislikes());
			postOut.writeKey("children", ofList(lazyPostCodec), post.getChildren());
		});
		lazyPostCodec.realize(StructuredCodec.of($ -> null, postEncoder));
		lazyPostCodec.encode(out, root);
	};

}
