package io.global.video.dao;

import io.datakernel.async.Promise;
import io.datakernel.ot.OTStateManager;
import io.global.ot.api.CommitId;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.video.Utils;
import io.global.video.pojo.Comment;
import io.global.video.pojo.UserId;

import java.util.Map;
import java.util.Set;

import static io.global.ot.map.SetValue.set;
import static java.util.stream.Collectors.toMap;

public final class CommentDaoImpl implements CommentDao {
	private final OTStateManager<CommitId, MapOperation<Long, Comment>> stateManager;
	private final Map<Long, Comment> stateView;

	public CommentDaoImpl(OTStateManager<CommitId, MapOperation<Long, Comment>> stateManager) {
		this.stateManager = stateManager;
		this.stateView = ((MapOTState<Long, Comment>) stateManager.getState()).getMap();
	}

	@Override
	public Promise<Void> addComment(UserId author, String content) {
		Long commentId = Utils.generateCommentId();
		while (stateView.containsKey(commentId)) {
			commentId = Utils.generateCommentId();
		}
		Comment comment = new Comment(author, content, System.currentTimeMillis());
		stateManager.add(MapOperation.forKey(commentId, set(null, comment)));
		return stateManager.sync();
	}

	@Override
	public Promise<Map<Long, Comment>> listComments() {
		return Promise.of(stateView);
	}

	@Override
	public Promise<Void> removeComments(Set<Long> commentIds) {
		Map<Long, SetValue<Comment>> removeMap = stateView.entrySet().stream()
				.filter(entry -> commentIds.contains(entry.getKey()))
				.collect(toMap(Map.Entry::getKey, entry -> SetValue.set(entry.getValue(), null)));
		stateManager.add(MapOperation.of(removeMap));
		return stateManager.sync();
	}
}
