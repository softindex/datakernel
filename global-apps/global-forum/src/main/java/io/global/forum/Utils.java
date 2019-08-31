package io.global.forum;

import com.github.mustachejava.Mustache;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredEncoder;
import io.datakernel.codec.StructuredOutput;
import io.datakernel.codec.registry.CodecRegistry;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.util.Tuple2;
import io.datakernel.writer.ByteBufWriter;
import io.global.forum.http.IpBanRequest;
import io.global.forum.ot.ForumMetadata;
import io.global.forum.ot.post.operation.*;
import io.global.forum.ot.session.operation.AddOrRemoveSession;
import io.global.forum.ot.session.operation.SessionOperation;
import io.global.forum.ot.session.operation.UpdateTimestamp;
import io.global.forum.pojo.*;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.*;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.StructuredEncoder.ofObject;
import static io.datakernel.http.ContentTypes.HTML_UTF_8;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpHeaders.REFERER;
import static io.global.ot.OTUtils.*;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	@SuppressWarnings("ConstantConditions") // - UserData::new is a false positive of the inspection?
	public static final CodecRegistry REGISTRY = createOTRegistry()
			.with(Instant.class, LONG_CODEC.transform(Instant::ofEpochMilli, Instant::toEpochMilli))
			.with(ThreadMetadata.class, tuple(ThreadMetadata::new,
					ThreadMetadata::getTitle, STRING_CODEC))
			.with(UserId.class, registry -> tuple(UserId::new,
					UserId::getAuthService, ofEnum(AuthService.class),
					UserId::getId, STRING_CODEC))
			.with(UserData.class, registry -> tuple(UserData::new,
					UserData::getRole, ofEnum(UserRole.class),
					UserData::getEmail, STRING_CODEC.nullable(),
					UserData::getUsername, STRING_CODEC.nullable(),
					UserData::getFirstName, STRING_CODEC.nullable(),
					UserData::getLastName, STRING_CODEC.nullable(),
					UserData::getBanState, registry.get(BanState.class).nullable()))
			.with(IpRange.class, registry -> tuple(IpRange::new,
					IpRange::getLowerBound, LONG_CODEC,
					IpRange::getUpperBound, LONG_CODEC))
			.with(BanState.class, registry -> tuple(BanState::new,
					BanState::getBanner, registry.get(UserId.class),
					BanState::getUntil, registry.get(Instant.class),
					BanState::getReason, STRING_CODEC))
			.with(IpBanState.class, registry -> tuple(IpBanState::new,
					IpBanState::getBanState, registry.get(BanState.class),
					IpBanState::getIpRange, registry.get(IpRange.class)))
			.with(Attachment.class, registry -> tuple(Attachment::new,
					Attachment::getAttachmentType, ofEnum(AttachmentType.class),
					Attachment::getFileName, STRING_CODEC))
			.with(ForumMetadata.class, registry -> tuple(ForumMetadata::new,
					ForumMetadata::getName, STRING_CODEC,
					ForumMetadata::getDescription, STRING_CODEC))
			.with(AddPost.class, registry -> tuple(AddPost::new,
					AddPost::getPostId, registry.get(Long.class),
					AddPost::getParentId, LONG_CODEC.nullable(),
					AddPost::getAuthor, registry.get(UserId.class),
					AddPost::getInitialTimestamp, LONG_CODEC,
					AddPost::isRemove, BOOLEAN_CODEC))
			.with(ChangeAttachments.class, registry -> tuple(ChangeAttachments::new,
					ChangeAttachments::getPostId, LONG_CODEC,
					ChangeAttachments::getGlobalFsId, STRING_CODEC,
					ChangeAttachments::getAttachment, registry.get(Attachment.class),
					ChangeAttachments::getTimestamp, LONG_CODEC,
					ChangeAttachments::isRemove, BOOLEAN_CODEC))
			.with(ChangeContent.class, registry -> tuple(ChangeContent::new,
					ChangeContent::getPostId, LONG_CODEC,
					ChangeContent::getChangeContent, CHANGE_NAME_CODEC))
			.with(ChangeLastEditTimestamp.class, registry -> tuple(ChangeLastEditTimestamp::new,
					ChangeLastEditTimestamp::getPostId, LONG_CODEC,
					ChangeLastEditTimestamp::getPrevTimestamp, LONG_CODEC,
					ChangeLastEditTimestamp::getNextTimestamp, LONG_CODEC))
			.with(ChangeRating.class, registry -> tuple(ChangeRating::new,
					ChangeRating::getPostId, LONG_CODEC,
					ChangeRating::getUserId, registry.get(UserId.class),
					ChangeRating::getSetRating, getSetValueCodec(BOOLEAN_CODEC)))
			.with(DeletePost.class, registry -> tuple(DeletePost::new,
					DeletePost::getPostId, LONG_CODEC,
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
			.with(IpBanRequest.class, registry -> object(IpBanRequest::new,
					"range", IpBanRequest::getRange, registry.get(IpRange.class),
					"until", IpBanRequest::getUntil, registry.get(Instant.class),
					"description", IpBanRequest::getDescription, STRING_CODEC))
			.with(AddOrRemoveSession.class, registry -> tuple(AddOrRemoveSession::of,
					AddOrRemoveSession::getSessionId, STRING_CODEC,
					AddOrRemoveSession::getUserId, registry.get(UserId.class),
					AddOrRemoveSession::getTimestamp, LONG_CODEC,
					AddOrRemoveSession::isRemove, BOOLEAN_CODEC))
			.with(UpdateTimestamp.class, registry -> tuple(UpdateTimestamp::update,
					UpdateTimestamp::getSessionId, STRING_CODEC,
					UpdateTimestamp::getPrevious, LONG_CODEC,
					UpdateTimestamp::getNext, LONG_CODEC))
			.with(SessionOperation.class, registry -> CodecSubtype.<SessionOperation>create()
					.with(AddOrRemoveSession.class, registry.get(AddOrRemoveSession.class))
					.with(UpdateTimestamp.class, registry.get(UpdateTimestamp.class)));

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

	@FunctionalInterface
	public interface MustacheSupplier {

		Mustache getMustache(String filename);
	}

	public static final class MustacheTemplater {
		private final MustacheSupplier mustacheSupplier;
		private final Map<String, Object> staticContext = new HashMap<>();

		public MustacheTemplater(MustacheSupplier mustacheSupplier) {
			this.mustacheSupplier = mustacheSupplier;
		}

		public Map<String, Object> getStaticContext() {
			return staticContext;
		}

		public Promise<HttpResponse> render(int code, String templateName, Map<String, Object> scope) {
			ByteBufWriter writer = new ByteBufWriter();

			scope.putAll(staticContext);

			List<Promise<?>> promisesToWait = new ArrayList<>();

			for (Map.Entry<String, Object> entry : scope.entrySet()) {
				Object value = entry.getValue();
				if (value instanceof Promise) {
					Promise<?> promise = (Promise<?>) value;
					if (promise.isResult()) {
						entry.setValue(promise.getResult());
					} else {
						promisesToWait.add(promise.whenResult(entry::setValue));
					}
				}
			}
			return Promises.all(promisesToWait)
					.map($ -> {
						mustacheSupplier.getMustache(templateName + ".mustache").execute(writer, scope);
						return HttpResponse.ofCode(code)
								.withBody(writer.getBuf())
								.withHeader(CONTENT_TYPE, ofContentType(HTML_UTF_8));
					});
		}

		public Promise<HttpResponse> render(String templateName, Map<String, Object> scope) {
			return render(200, templateName, scope);
		}
	}

	public static HttpResponse redirect(HttpRequest request, @NotNull String to) {
		String referer = request.getHeader(REFERER);
		return HttpResponse.redirect302(referer == null ? to : referer);
	}

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

	public static final StructuredEncoder<Tuple2<Map<Long, Post>, Long>> RECURSIVE_POST_ENCODER;

	public static final StructuredEncoder<Post> POST_SIMPLE_ENCODER = (out, post) -> {
		StructuredCodec<UserId> userIdCodec = REGISTRY.get(UserId.class);
		StructuredCodec<Set<UserId>> userIdCodecSet = ofSet(userIdCodec);
		out.writeKey("author", userIdCodec, post.getAuthor());
		out.writeKey("created", LONG_CODEC, post.getInitialTimestamp());
		out.writeKey("content", STRING_CODEC, post.getContent());
		out.writeKey("attachments", ofMap(STRING_CODEC, REGISTRY.get(Attachment.class)), post.getAttachments());
		out.writeKey("deletedBy", userIdCodec.nullable(), post.getDeletedBy());
		out.writeKey("edited", LONG_CODEC, post.getLastEditTimestamp());
		out.writeKey("likes", userIdCodecSet, post.getLikes());
		out.writeKey("dislikes", userIdCodecSet, post.getDislikes());
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
			out.writeKey("children", StructuredEncoder.ofList(lazyPostEncoder), childrenEntries);
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
