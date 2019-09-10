package io.global.forum.util;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredEncoder;
import io.datakernel.codec.registry.CodecRegistry;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.util.Tuple2;
import io.global.forum.http.IpBanRequest;
import io.global.forum.ot.ForumMetadata;
import io.global.forum.ot.post.operation.*;
import io.global.forum.ot.session.operation.AddOrRemoveSession;
import io.global.forum.ot.session.operation.SessionOperation;
import io.global.forum.ot.session.operation.UpdateTimestamp;
import io.global.forum.pojo.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.StructuredEncoder.ofObject;
import static io.datakernel.http.HttpHeaders.REFERER;
import static io.global.ot.OTUtils.*;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final CodecRegistry REGISTRY = createOTRegistry()
			.with(Instant.class, LONG_CODEC.transform(Instant::ofEpochMilli, Instant::toEpochMilli))
			.with(ThreadMetadata.class, tuple(ThreadMetadata::new,
					ThreadMetadata::getTitle, STRING_CODEC))
			.with(UserId.class, registry -> tuple(UserId::new,
					UserId::getAuthService, ofEnum(AuthService.class),
					UserId::getAuthId, STRING_CODEC))
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
					Attachment::getFilename, STRING_CODEC))
			.with(ForumMetadata.class, registry -> tuple(ForumMetadata::new,
					ForumMetadata::getName, STRING_CODEC,
					ForumMetadata::getDescription, STRING_CODEC))
			.with(AddPost.class, registry -> tuple(AddPost::new,
					AddPost::getPostId, STRING_CODEC,
					AddPost::getParentId, STRING_CODEC.nullable(),
					AddPost::getAuthor, registry.get(UserId.class),
					AddPost::getInitialTimestamp, LONG_CODEC,
					AddPost::isRemove, BOOLEAN_CODEC))
			.with(ChangeAttachments.class, registry -> tuple(ChangeAttachments::new,
					ChangeAttachments::getPostId, STRING_CODEC,
					ChangeAttachments::getGlobalFsId, STRING_CODEC,
					ChangeAttachments::getAttachment, registry.get(Attachment.class),
					ChangeAttachments::getTimestamp, LONG_CODEC,
					ChangeAttachments::isRemove, BOOLEAN_CODEC))
			.with(ChangeContent.class, registry -> tuple(ChangeContent::new,
					ChangeContent::getPostId, STRING_CODEC,
					ChangeContent::getChangeContent, CHANGE_NAME_CODEC))
			.with(ChangeLastEditTimestamp.class, registry -> tuple(ChangeLastEditTimestamp::new,
					ChangeLastEditTimestamp::getPostId, STRING_CODEC,
					ChangeLastEditTimestamp::getPrevTimestamp, LONG_CODEC,
					ChangeLastEditTimestamp::getNextTimestamp, LONG_CODEC))
			.with(ChangeRating.class, registry -> tuple(ChangeRating::new,
					ChangeRating::getPostId, STRING_CODEC,
					ChangeRating::getUserId, registry.get(UserId.class),
					ChangeRating::getSetRating, getSetValueCodec(BOOLEAN_CODEC)))
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
			.with(IpBanRequest.class, registry -> tuple(IpBanRequest::new,
					IpBanRequest::getRange, registry.get(IpRange.class),
					IpBanRequest::getUntil, registry.get(Instant.class),
					IpBanRequest::getDescription, STRING_CODEC))
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
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'
	};

	private static final StructuredEncoder<Post> POST_SIMPLE_ENCODER = (out, post) -> {
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

	public static HttpResponse redirectToReferer(HttpRequest request, String defaultPath) {
		String referer = request.getHeader(REFERER);
		return HttpResponse.redirect302(referer != null ? referer : defaultPath);
	}

	public static <T, E> BiFunction<T, Throwable, Promise<T>> revertIfException(AsyncSupplier<E> undo) {
		return (result, e) -> {
			if (e == null) {
				return Promise.of(result);
			}
			return undo.get()
					.thenEx(($2, e2) -> {
						if (e2 != null) {
							e.addSuppressed(e2);
						}
						return Promise.ofException(e);
					});
		};
	}
}
