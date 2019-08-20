package io.global.forum.dao;

import io.datakernel.async.Promise;
import io.datakernel.ot.OTStateManager;
import io.global.forum.Utils;
import io.global.forum.pojo.Post;
import io.global.forum.pojo.UserId;
import io.global.ot.api.CommitId;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;

import java.util.Map;
import java.util.Set;

import static io.global.ot.map.SetValue.set;
import static java.util.stream.Collectors.toMap;

public final class PostDaoImpl implements PostDao {
	private final OTStateManager<CommitId, MapOperation<Long, Post>> stateManager;
	private final Map<Long, Post> stateView;

	public PostDaoImpl(OTStateManager<CommitId, MapOperation<Long, Post>> stateManager) {
		this.stateManager = stateManager;
		this.stateView = ((MapOTState<Long, Post>) stateManager.getState()).getMap();
	}

	@Override
	public Promise<Void> addComment(UserId author, String content) {
		Long commentId = Utils.generateCommentId();
		while (stateView.containsKey(commentId)) {
			commentId = Utils.generateCommentId();
		}
		Post post = new Post(author, content, System.currentTimeMillis());
		stateManager.add(MapOperation.forKey(commentId, set(null, post)));
		return stateManager.sync();
	}

	@Override
	public Promise<Map<Long, Post>> listComments() {
		return Promise.of(stateView);
	}

	@Override
	public Promise<Void> removeComments(Set<Long> commentIds) {
		Map<Long, SetValue<Post>> removeMap = stateView.entrySet().stream()
				.filter(entry -> commentIds.contains(entry.getKey()))
				.collect(toMap(Map.Entry::getKey, entry -> SetValue.set(entry.getValue(), null)));
		stateManager.add(MapOperation.of(removeMap));
		return stateManager.sync();
	}
}
