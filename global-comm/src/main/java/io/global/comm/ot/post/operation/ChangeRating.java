package io.global.comm.ot.post.operation;

import io.global.comm.pojo.Post;
import io.global.comm.pojo.Rating;
import io.global.comm.pojo.UserId;
import io.global.ot.map.SetValue;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public final class ChangeRating implements ThreadOperation {
	private final String postId;
	private final UserId userId;
	private final SetValue<Rating> setRating;

	public ChangeRating(String postId, UserId userId, SetValue<Rating> setRating) {
		this.postId = postId;
		this.userId = userId;
		this.setRating = setRating;
	}

	@Override
	public void apply(Map<String, Post> posts) {
		Post post = posts.get(postId);
		post.updateRating(userId, setRating.getNext());
	}

	public String getPostId() {
		return postId;
	}

	public UserId getUserId() {
		return userId;
	}

	public SetValue<Rating> getSetRating() {
		return setRating;
	}

	private String toHumanReadable(@Nullable Rating value) {
		return value == null ? "not-rated" : value.name().toLowerCase();
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
