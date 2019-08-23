package io.global.forum.ot.post.operation;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.UserId;
import io.global.ot.map.SetValue;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static io.datakernel.codec.StructuredCodecs.BOOLEAN_CODEC;
import static io.datakernel.codec.StructuredCodecs.LONG_CODEC;
import static io.global.ot.OTUtils.getSetValueCodec;

public final class ChangeRating implements PostOperation {
	public static final StructuredCodec<ChangeRating> CODEC = StructuredCodecs.tuple(ChangeRating::new,
			ChangeRating::getPostId, LONG_CODEC,
			ChangeRating::getUserId, UserId.CODEC,
			ChangeRating::getSetRating, getSetValueCodec(BOOLEAN_CODEC));

	private final long postId;
	private final UserId userId;
	private final SetValue<Boolean> setRating;

	public ChangeRating(long postId, UserId userId, SetValue<Boolean> setRating) {
		this.postId = postId;
		this.userId = userId;
		this.setRating = setRating;
	}

	@Override
	public void apply(Map<Long, Post> posts) {
		Post post = posts.get(postId);
		Boolean next = setRating.getNext();
		if (next == null) {
			post.removeLikeAndDislike(userId);
		} else if (next) {
			post.addLike(userId);
		} else {
			post.addDislike(userId);
		}
	}

	public long getPostId() {
		return postId;
	}

	public UserId getUserId() {
		return userId;
	}

	public SetValue<Boolean> getSetRating() {
		return setRating;
	}

	private String toHumanReadable(@Nullable Boolean value) {
		return value == null ? "nothing" : value ? "like" : "dislike";
	}

	@Override
	public String toString() {
		return "ChangeRating{" +
				"postId=" + postId +
				", userId=" + userId +
				", from=" + toHumanReadable(setRating.getPrev()) +
				", to=" + toHumanReadable(setRating.getNext()) +
				'}';
	}
}
