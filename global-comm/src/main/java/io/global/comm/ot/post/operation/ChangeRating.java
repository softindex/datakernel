package io.global.comm.ot.post.operation;

import io.global.comm.pojo.Post;
import io.global.ot.map.SetValue;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public final class ChangeRating implements ThreadOperation {
	private final String postId;
	private final UserId userId;
	private final SetValue<Boolean> setRating;

	public ChangeRating(String postId, UserId userId, SetValue<Boolean> setRating) {
		this.postId = postId;
		this.userId = userId;
		this.setRating = setRating;
	}

	@Override
	public void apply(Map<String, Post> posts) {
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

	public String getPostId() {
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
